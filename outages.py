import unittest
from datetime import date

import httpx

from app.storage import connect
from tests.live import ANALYTICS, PRODUCER, archive, manual, pg_metrics, publish, raw_event, wait_for
from tests.sample import event

EXPORT_DAY = date(2020, 7, 2)
PG_DAY = date(2020, 7, 3)


class BrokerDown(unittest.TestCase):
    def test_accept_and_consume_with_two_brokers(self):
        data = event("failover", "film", "VIEW_STARTED", date(2020, 7, 1))
        def acknowledged():
            response = httpx.post(f"{PRODUCER}/events", json=data, timeout=35)
            if response.status_code == 503:
                return False
            self.assertEqual(response.status_code, 201)
            return response.json()["event_id"] == data["event_id"]
        wait_for(acknowledged)
        self.assertEqual(wait_for(lambda: raw_event(data["event_id"]))["user_id"], "failover")


class MinioDown(unittest.TestCase):
    def test_failed_export_is_queued(self):
        data = event("delayed-export", "film", "VIEW_STARTED", EXPORT_DAY)
        publish(data)
        wait_for(lambda: raw_event(data["event_id"]))
        manual("recalculate", EXPORT_DAY)
        response = httpx.post(f"{ANALYTICS}/export/{EXPORT_DAY}", timeout=60)
        self.assertEqual(response.status_code, 503)
        self.assertEqual(pg_metrics(EXPORT_DAY)["dau", ""], 1)
        with connect() as conn:
            row = conn.execute("SELECT available_on FROM export_queue WHERE metric_date = %s", (EXPORT_DAY,)).fetchone()
        self.assertIsNotNone(row)


class MinioRecovered(unittest.TestCase):
    def test_scheduler_retries_after_restart(self):
        document = wait_for(lambda: archive(EXPORT_DAY))
        self.assertEqual(document["date"], EXPORT_DAY.isoformat())
        self.assertEqual(next(r["value"] for r in document["metrics"] if r["metric_name"] == "dau"), 1)


class PostgresDown(unittest.TestCase):
    def test_ingestion_continues(self):
        data = event("pg-recovery", "film", "VIEW_STARTED", PG_DAY)
        publish(data)
        wait_for(lambda: raw_event(data["event_id"]))
        response = httpx.post(f"{ANALYTICS}/recalculate/{PG_DAY}", timeout=45)
        self.assertEqual(response.status_code, 503)


class PostgresRecovered(unittest.TestCase):
    def test_scheduler_catches_up(self):
        document = wait_for(lambda: archive(PG_DAY))
        self.assertEqual(document["date"], PG_DAY.isoformat())
        self.assertEqual(pg_metrics(PG_DAY)["dau", ""], 1)
