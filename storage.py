import os
from datetime import UTC, datetime
from time import monotonic

import psycopg

from app import clickhouse
from app.common import ROOT, log, retry
from app.metrics import calculate, metric_values

PG_DSN = os.getenv("PG_DSN", "postgresql://cinema:cinema_demo@postgres:5432/cinema")

UPSERT = """
INSERT INTO daily_metrics (metric_date, metric_name, dimension, value, computed_at)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (metric_date, metric_name, dimension)
DO UPDATE SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at
"""


def connect():
    return psycopg.connect(PG_DSN, connect_timeout=5)


def migrate():
    with connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(3401, 0)")
        for script in sorted((ROOT / "sql/postgres").glob("*.sql")):
            conn.execute(script.read_text())


def lock_day(conn, day):
    conn.execute("SELECT pg_advisory_xact_lock(%s, %s)", (3401, day.toordinal()))


def recalculate(day):
    started = monotonic()
    log.info("calculation started date=%s", day)

    def save():
        with connect() as conn:
            lock_day(conn, day)
            stamp = datetime.now(UTC)
            snapshot = calculate(day, stamp, clickhouse.query)
            values = metric_values(snapshot)
            rows = conn.execute(
                "SELECT metric_name, dimension, value FROM daily_metrics WHERE metric_date = %s", (day,)
            )
            previous = {(name, dimension): value for name, dimension, value in rows}
            if previous != values:
                clickhouse.query("INSERT INTO cinema.daily_metrics FORMAT JSONEachRow", data=snapshot)
                with conn.cursor() as cursor:
                    cursor.executemany(UPSERT, [
                        (day, name, dimension, value, stamp) for (name, dimension), value in values.items()
                    ])
                    cursor.executemany(
                        "DELETE FROM daily_metrics WHERE metric_date = %s AND metric_name = %s AND dimension = %s",
                        [(day, name, dimension) for name, dimension in previous.keys() - values.keys()]
                    )
                conn.execute(
                    "INSERT INTO export_queue (metric_date, available_on) VALUES (%s, %s::date + 1) "
                    "ON CONFLICT DO NOTHING", (day, day)
                )
            return snapshot["event_count"]

    try:
        count = retry(save, (psycopg.OperationalError, psycopg.InterfaceError))
    except Exception:
        log.exception("calculation failed date=%s elapsed=%.3fs", day, monotonic() - started)
        raise
    elapsed = round(monotonic() - started, 3)
    log.info("calculation finished date=%s events=%d elapsed=%.3fs", day, count, elapsed)
    return {"date": day, "events_processed": count, "elapsed_seconds": elapsed}
