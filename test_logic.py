import json
import random
import sqlite3
import unittest
from collections import defaultdict
from datetime import UTC, date, datetime, time, timedelta
from typing import get_args
from unittest.mock import patch

from pydantic import ValidationError

from app.common import ROOT, retry
from app.events import DeviceType, EventType, MovieEvent
from app.generator import history, session
from app.metrics import calculate, metric_values
from tests.sample import DAY, event, sample_events


class EventTests(unittest.TestCase):
    def test_schema_and_utc(self):
        schema = json.loads((ROOT / "schemas/movie-event-v1.avsc").read_text())
        fields = {f["name"]: f["type"] for f in schema["fields"]}
        self.assertEqual(set(fields), set(MovieEvent.model_fields))
        self.assertEqual(set(fields["event_type"]["symbols"]), set(get_args(EventType)))
        self.assertEqual(set(fields["device_type"]["symbols"]), set(get_args(DeviceType)))
        self.assertEqual(fields["event_id"]["logicalType"], "uuid")
        self.assertEqual(fields["timestamp"]["logicalType"], "timestamp-millis")
        data = event("one", "film", "VIEW_STARTED")
        data["timestamp"] = "2020-04-02T00:30:10.123987+03:00"
        normalized = MovieEvent(**data)
        self.assertEqual(normalized.timestamp.isoformat(), "2020-04-01T21:30:10.123000+00:00")

    def test_rejects_invalid_events(self):
        data = event("one", "film", "LIKED")
        for name, value in (("event_id", "bad"), ("user_id", " "), ("session_id", ""),
                            ("device_type", "WATCH"), ("event_type", "NEW"), ("progress_seconds", -1),
                            ("progress_seconds", 2**31), ("progress_seconds", True), ("progress_seconds", 0.5),
                            ("timestamp", "2020-04-01T12:00:00"), ("timestamp", 100), ("timestamp", "1234567890"),
                            ("timestamp", "0001-01-01T00:00:00+14:00"), ("wrong", 1)):
            with self.subTest(name=name, value=value), self.assertRaises(ValidationError):
                MovieEvent(**{**data, name: value})
        for name in data:
            with self.subTest(missing=name), self.assertRaises(ValidationError):
                MovieEvent(**{key: value for key, value in data.items() if key != name})

    def test_generator_order_and_progress(self):
        seen = set()
        start = datetime(2020, 1, 1, tzinfo=UTC)
        for number in range(250):
            items = list(session("user", start, str(number), random.Random(number)))
            seen.update(item.event_type for item in items)
            times = [item.timestamp for item in items]
            self.assertEqual(times, sorted(set(times)))
            views = [item for item in items if item.event_type.startswith("VIEW_")]
            self.assertEqual(views[0].event_type, "VIEW_STARTED")
            progress = [item.progress_seconds for item in views]
            self.assertEqual(progress, sorted(set(progress)))
            kinds = [item.event_type for item in views]
            if "VIEW_RESUMED" in kinds:
                self.assertLess(kinds.index("VIEW_PAUSED"), kinds.index("VIEW_RESUMED"))
            if "VIEW_FINISHED" in kinds:
                self.assertEqual(kinds[-1], "VIEW_FINISHED")
        self.assertEqual(seen, set(get_args(EventType)))

    def test_history_retry_keeps_ids_and_returning_users(self):
        first = list(history(DAY))
        self.assertEqual(first, list(history(DAY)))
        self.assertEqual(len(first), len({item.event_id for item in first}))
        days = defaultdict(set)
        for item in first:
            self.assertLess(item.timestamp.date(), DAY)
            days[item.user_id].add(item.timestamp.date())
        self.assertTrue(any((max(dates) - min(dates)).days == 7 for dates in days.values()))

    @patch("app.common.time.sleep")
    def test_retry_backoff(self, sleep):
        failures = iter([OSError(), OSError(), "ok"])
        def write():
            result = next(failures)
            if isinstance(result, Exception):
                raise result
            return result
        self.assertEqual(retry(write, OSError), "ok")
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [0.5, 1])


class Unique:
    def __init__(self):
        self.values = set()

    def step(self, value):
        self.values.add(value)

    def finalize(self):
        return len(self.values)


class SumIf:
    def __init__(self):
        self.total = 0

    def step(self, value, condition):
        if condition:
            self.total += value

    def finalize(self):
        return self.total


class CountIf(SumIf):
    def step(self, condition):
        super().step(1, condition)


class MetricTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.row_factory = sqlite3.Row
        self.db.execute("ATTACH DATABASE ':memory:' AS cinema")
        self.db.execute("CREATE TABLE cinema.events (user_id, movie_id, event_type, event_date, progress_seconds)")
        self.db.create_aggregate("uniqExact", 1, Unique)
        self.db.create_aggregate("sumIf", 2, SumIf)
        self.db.create_aggregate("countIf", 1, CountIf)
        self.db.create_function("dateDiff", 3, lambda unit, a, b: (date.fromisoformat(b) - date.fromisoformat(a)).days)
        self.db.create_function("addDays", 2, lambda day, n: (date.fromisoformat(day) + timedelta(days=n)).isoformat())
        for data in sample_events():
            item = MovieEvent(**data)
            self.db.execute("INSERT INTO cinema.events VALUES (?, ?, ?, ?, ?)", (
                item.user_id, item.movie_id, item.event_type, item.timestamp.date().isoformat(), item.progress_seconds
            ))

    def read(self, sql, day):
        return [dict(row) for row in self.db.execute(sql.replace("{day:Date}", ":day"), {"day": day.isoformat()})]

    def snapshot(self, day=DAY, now=DAY + timedelta(days=8)):
        return calculate(day, datetime.combine(now, time(12), UTC), self.read)

    def test_metrics_and_retention(self):
        data = self.snapshot()
        self.assertEqual((data["event_count"], data["dau"], data["avg_watch_seconds"]), (9, 4, 900))
        self.assertAlmostEqual(data["conversion"], 2/3)
        self.assertEqual(data["cohort_size"], 2)
        self.assertEqual(data["retention"], [1, .5, .5, 0, 0, 0, 0, .5])
        self.assertEqual(data["movie_ids"], ["a", "b"])
        self.assertEqual(data["movie_views"], [2, 1])
        values = metric_values(data)
        self.assertEqual(values["retention_d1", ""], .5)
        self.assertEqual(values["retention_d7", ""], .5)
        self.assertEqual(values["cohort_retention", "4"], 0)
        self.assertEqual(values["movie_views", "a"], 2)

    def test_unobserved_days(self):
        self.assertEqual(self.snapshot(now=DAY + timedelta(days=1))["retention"], [1, .5] + [None] * 6)

    def test_empty_day(self):
        data = self.snapshot(day=DAY + timedelta(days=4))
        self.assertEqual((data["event_count"], data["dau"]), (0, 0))
        self.assertIsNone(data["conversion"])
        self.assertIsNone(data["avg_watch_seconds"])
        self.assertEqual(data["cohort_size"], 0)
        self.assertEqual(data["retention"], [None] * 8)
        self.assertEqual(data["movie_ids"], [])

    def test_search_only_day(self):
        data = self.snapshot(day=DAY - timedelta(days=2))
        self.assertEqual(data["dau"], 1)
        self.assertEqual(data["cohort_size"], 0)
        self.assertIsNone(data["conversion"])
        self.assertIsNone(data["avg_watch_seconds"])


    def test_finish_with_no_starts_that_day(self):
        finished_day = DAY + timedelta(days=1)
        self.db.execute("INSERT INTO cinema.events VALUES (?, ?, ?, ?, ?)",
                        ("anna", "a", "VIEW_FINISHED", finished_day.isoformat(), 1800))
        data = self.snapshot(day=finished_day)
        self.assertEqual(data["avg_watch_seconds"], 1800)
        self.assertIsNone(data["conversion"])
        self.assertEqual(data["movie_ids"], [])
