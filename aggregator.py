import os
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from threading import Event, Thread
from time import monotonic

from fastapi import FastAPI, HTTPException

from app import clickhouse
from app.common import enabled, log
from app.events import MIN_DATE, MAX_DATE
from app.export import NoMetrics, export_pending, request_export
from app.storage import connect, migrate, recalculate


def calculation_cycle():
    started = monotonic()
    log.info("scheduled calculation started")
    today = datetime.now(UTC).date()
    yesterday = today - timedelta(days=1)
    raw_days = clickhouse.query(
        "SELECT DISTINCT event_date FROM cinema.events WHERE event_date <= {day:Date}", today
    )
    days = {today, yesterday, *(date.fromisoformat(row["event_date"]) for row in raw_days)}
    with connect() as conn:
        last, = conn.execute(
            "SELECT max(metric_date) FROM daily_metrics WHERE metric_date <= %s", (yesterday,)
        ).fetchone()
    if last:
        days.update(last + timedelta(days=i) for i in range(1, (yesterday - last).days))
    count = 0
    failed = 0
    for day in sorted(days):
        try:
            count += recalculate(day)["events_processed"]
        except Exception:
            failed += 1
            log.error("date=%s will be retried in next cycle", day)
    log.info("scheduled calculation finished days=%d failed=%d events=%d elapsed=%.3fs",
             len(days), failed, count, monotonic() - started)


def schedule(stop):
    interval = max(1, int(os.getenv("AGG_INTERVAL_SECONDS", "60")))
    while not stop.is_set():
        for job in (calculation_cycle, export_pending):
            try:
                job()
            except Exception:
                log.exception("scheduled job failed job=%s", job.__name__)
        stop.wait(interval)


@asynccontextmanager
async def lifespan(app):
    migrate()
    stop = Event()
    worker = None
    if enabled("SCHEDULER_ENABLED"):
        worker = Thread(target=schedule, args=(stop,), daemon=True)
        worker.start()
    try:
        yield
    finally:
        stop.set()
        if worker:
            worker.join(timeout=8)


app = FastAPI(title="Cinema Analytics", lifespan=lifespan)


@app.get("/health")
def health():
    try:
        clickhouse.query("SELECT count() FROM cinema.daily_metrics WHERE 0")
        with connect() as conn:
            conn.execute("SELECT 1 FROM daily_metrics LIMIT 0")
    except Exception as exc:
        raise HTTPException(503, "Хранилище метрик недоступно") from exc
    return {"status": "ok"}


def check_date(day):
    if not MIN_DATE <= day <= min(MAX_DATE, datetime.now(UTC).date()):
        raise HTTPException(422, "Нужна прошедшая или текущая дата UTC в диапазоне ClickHouse Date")


@app.post("/recalculate/{day}")
def recalculate_http(day: date):
    check_date(day)
    try:
        return recalculate(day)
    except Exception as exc:
        raise HTTPException(503, "Не удалось пересчитать метрики") from exc


@app.post("/export/{day}")
def export_http(day: date):
    check_date(day)
    try:
        return request_export(day)
    except NoMetrics as exc:
        raise HTTPException(404, "За эту дату ещё нет рассчитанных метрик") from exc
    except Exception as exc:
        log.exception("export request failed date=%s", day)
        raise HTTPException(503, "Не удалось выгрузить метрики") from exc
