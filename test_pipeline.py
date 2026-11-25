import io
import json
import struct
import unittest
from datetime import UTC, date, datetime, timedelta
from time import monotonic
from uuid import uuid4

import httpx
from confluent_kafka import Consumer, OFFSET_BEGINNING, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource
from fastavro import schemaless_reader

from app import clickhouse
from app.broker import BROKERS, REGISTRY, SCHEMA, SUBJECT, TOPIC
from app.events import MovieEvent
from app.export import BUCKET, s3_client
from app.storage import connect
from tests.live import ANALYTICS, GRAFANA, PRODUCER, archive, manual, pg_metrics, publish, raw_event, wait_for
from tests.sample import DAY, event, sample_events


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.events = sample_events()
        for data in cls.events:
            publish(data)
        publish(cls.events[2])
        ids = {data["event_id"] for data in cls.events}
        def all_arrived():
            rows = clickhouse.query("SELECT toString(event_id) AS id FROM cinema.events")
            return ids <= {row["id"] for row in rows}
        wait_for(all_arrived)
        report = manual("recalculate", DAY)
        if report["events_processed"] != 9:
            raise AssertionError(report)

    def test_raw_event_all_fields(self):
        data = self.events[3]
        item = MovieEvent(**data)
        row = raw_event(data["event_id"])
        self.assertEqual(row, {"id": data["event_id"], "user_id": data["user_id"],
                               "movie_id": data["movie_id"], "event_type": data["event_type"],
                               "ms": int(item.timestamp.timestamp() * 1000), "device_type": data["device_type"],
                               "session_id": data["session_id"], "progress_seconds": data["progress_seconds"]})

    def test_api_validation(self):
        for update in ({"event_id": "invalid"}, {"event_type": "BAD"}, {"device_type": "CAR"},
                       {"timestamp": "2020-04-01T12:00:00"}, {"progress_seconds": -2}):
            with self.subTest(update=update):
                response = httpx.post(f"{PRODUCER}/events", json={**self.events[0], **update})
                self.assertEqual(response.status_code, 422)
        self.assertEqual(httpx.post(f"{ANALYTICS}/recalculate/bad").status_code, 422)
        self.assertEqual(httpx.post(f"{ANALYTICS}/export/1999-01-01").status_code, 404)

    def test_registry_replication_and_wire_format(self):
        stored = httpx.get(f"{REGISTRY}/subjects/{SUBJECT}/versions/1").raise_for_status().json()
        self.assertEqual(stored["version"], 1)
        self.assertEqual(json.loads(stored["schema"]), SCHEMA)
        admin = AdminClient({"bootstrap.servers": BROKERS})
        partitions = admin.list_topics(TOPIC, timeout=8).topics[TOPIC].partitions
        self.assertEqual(len(partitions), 3)
        self.assertTrue(all(len(p.replicas) == 3 for p in partitions.values()))
        resource = ConfigResource("TOPIC", TOPIC)
        config = admin.describe_configs([resource], request_timeout=8)[resource].result()
        self.assertEqual(config["min.insync.replicas"].value, "2")

        consumer = Consumer({"bootstrap.servers": BROKERS, "group.id": str(uuid4()), "enable.auto.commit": False})
        self.addCleanup(consumer.close)
        consumer.assign([TopicPartition(TOPIC, p, OFFSET_BEGINNING) for p in partitions])
        expected = [item["event_id"] for item in self.events[2:7]]
        found = {}
        deadline = monotonic() + 40
        while len(found) < len(expected) and monotonic() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            self.assertIsNone(message.error())
            stream = io.BytesIO(message.value())
            self.assertEqual(struct.unpack(">BI", stream.read(5)), (0, stored["id"]))
            record = schemaless_reader(stream, SCHEMA)
            event_id = str(record["event_id"])
            if event_id in expected and event_id not in found:
                self.assertEqual(message.key().decode(), "anna")
                found[event_id] = (message.partition(), message.offset())
        self.assertEqual(list(found), expected)
        self.assertEqual(len({part for part, offset in found.values()}), 1)
        offsets = [offset for part, offset in found.values()]
        self.assertEqual(offsets, sorted(offsets))

    def test_metrics_in_both_databases(self):
        for _ in range(2):
            self.assertEqual(manual("recalculate", DAY)["events_processed"], 9)
        data, = clickhouse.query("SELECT * FROM cinema.daily_metrics FINAL WHERE metric_date = {day:Date}", DAY)
        self.assertEqual((data["event_count"], data["dau"], data["avg_watch_seconds"]), (9, 4, 900))
        self.assertAlmostEqual(data["conversion"], 2 / 3)
        self.assertEqual(data["cohort_size"], 2)
        self.assertEqual(data["retention"], [1, .5, .5, 0, 0, 0, 0, .5])
        self.assertEqual(list(zip(data["movie_ids"], data["movie_views"])), [("a", 2), ("b", 1)])
        values = pg_metrics(DAY)
        self.assertEqual(len(values), 17)
        self.assertEqual(values["dau", ""], 4)
        self.assertEqual(values["avg_watch_seconds", ""], 900)
        self.assertEqual(values["retention_d1", ""], .5)
        self.assertEqual(values["retention_d7", ""], .5)
        self.assertAlmostEqual(values["conversion", ""], 2 / 3)
        self.assertEqual(values["movie_views", "a"], 2)
        self.assertEqual([values["cohort_retention", str(i)] for i in range(8)], data["retention"])

    def test_grafana_runs_provisioned_queries(self):
        admin = ("admin", "cinema_demo")
        def dashboard():
            response = httpx.get(f"{GRAFANA}/api/dashboards/uid/cinema-analytics", auth=admin)
            return response.json()["dashboard"] if response.status_code == 200 else None
        panels = wait_for(dashboard)["panels"]
        self.assertGreaterEqual(len(panels), 3)
        health = httpx.get(f"{GRAFANA}/api/datasources/uid/cinema-ch/health", auth=admin, timeout=20)
        self.assertEqual(health.raise_for_status().json()["status"], "OK")
        for panel in panels:
            with self.subTest(panel=panel["title"]):
                query = panel["targets"][0]
                self.assertIn("cinema.daily_metrics", query["rawSql"])
                bounds = [datetime.combine(d, datetime.min.time(), UTC) for d in (DAY, DAY + timedelta(days=1))]
                response = httpx.post(f"{GRAFANA}/api/ds/query", auth=admin, timeout=25, json={
                    "from": str(int(bounds[0].timestamp() * 1000)), "to": str(int(bounds[1].timestamp() * 1000) - 1),
                    "queries": [{**query, "intervalMs": 60000, "maxDataPoints": 1000}],
                })
                result = response.raise_for_status().json()["results"]["A"]
                self.assertFalse(result.get("error"), result)
                frame = result["frames"][0]
                self.assertTrue(frame["data"]["values"][0], panel["title"])
                if panel["title"] == "Retention Cohort Heatmap":
                    fields = [field["name"] for field in frame["schema"]["fields"]]
                    self.assertEqual(fields, ["Когорта"] + [f"Day {age}" for age in range(8)])
                    self.assertEqual([col[0] for col in frame["data"]["values"]],
                                     [DAY.isoformat(), 1, .5, .5, 0, 0, 0, 0, .5])

    def test_export_matches_postgres(self):
        key = f"daily/{DAY}/aggregates.json"
        self.assertEqual(manual("export", DAY)["key"], key)
        document = archive(DAY)
        self.assertEqual(document["date"], DAY.isoformat())
        with connect() as conn:
            rows = conn.execute(
                "SELECT metric_name, dimension, value, computed_at FROM daily_metrics WHERE metric_date = %s",
                (DAY,)
            ).fetchall()
        saved = {(row["metric_name"], row["dimension"], row["value"], datetime.fromisoformat(row["computed_at"]))
                 for row in document["metrics"]}
        self.assertEqual(saved, set(rows))

    def test_late_event_updates_existing_object(self):
        day = date(2020, 5, 3)
        for expected, viewer in enumerate(("first", "second"), start=1):
            data = event(viewer, "film", "VIEW_STARTED", day)
            publish(data)
            wait_for(lambda: raw_event(data["event_id"]))
            manual("recalculate", day)
            key = manual("export", day)["key"]
            self.assertEqual(key, f"daily/{day}/aggregates.json")
            document = archive(day)
            values = {row["metric_name"]: row["value"] for row in document["metrics"] if not row["dimension"]}
            self.assertEqual(values["dau"], expected)
            self.assertEqual(pg_metrics(day)["movie_views", "film"], expected)
        returning = event("second", "film", "LIKED", day + timedelta(days=7))
        publish(returning)

        def archive_has_return():
            metrics = archive(day)["metrics"]
            return any(row["metric_name"] == "retention_d7" and row["value"] == .5 for row in metrics)

        wait_for(archive_has_return)
        self.assertEqual(pg_metrics(day)["retention_d7", ""], .5)
        objects = s3_client().list_objects_v2(Bucket=BUCKET, Prefix=f"daily/{day}/")
        self.assertEqual([obj["Key"] for obj in objects["Contents"]], [key])

    def test_scheduled_calculation_and_export(self):
        day = date(2020, 6, 3)
        data = event("scheduled", "film", "VIEW_STARTED", day)
        publish(data)
        document = wait_for(lambda: archive(day))
        self.assertEqual(pg_metrics(day)["dau", ""], 1)
        self.assertEqual(next(row["value"] for row in document["metrics"] if row["metric_name"] == "dau"), 1)
