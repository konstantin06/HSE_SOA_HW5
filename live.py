import json
from datetime import date
from time import monotonic, sleep

import httpx
from botocore.exceptions import ClientError, EndpointConnectionError

from app import clickhouse
from app.export import BUCKET, s3_client
from app.storage import connect

PRODUCER = "http://producer:8000"
ANALYTICS = "http://aggregator:8000"
GRAFANA = "http://grafana:3000"


def wait_for(action, timeout=100):
    deadline = monotonic() + timeout
    last = None
    while monotonic() < deadline:
        try:
            result = action()
            if result:
                return result
        except (httpx.TransportError, EndpointConnectionError) as exc:
            last = exc
        sleep(0.5)
    raise AssertionError(f"Истекло время ожидания: {action.__name__}, последняя ошибка: {last}")


def publish(data):
    response = httpx.post(f"{PRODUCER}/events", json=data, timeout=35)
    response.raise_for_status()
    assert response.status_code == 201
    assert response.json() == {"event_id": data["event_id"]}


def raw_event(event_id):
    rows = clickhouse.query(
        "SELECT toString(event_id) AS id, user_id, movie_id, event_type, "
        "toUnixTimestamp64Milli(timestamp) AS ms, device_type, session_id, progress_seconds "
        "FROM cinema.events WHERE event_id = {id:UUID}", parameters={"id": event_id}
    )
    return rows[0] if rows else None


def pg_metrics(day):
    with connect() as conn:
        rows = conn.execute(
            "SELECT metric_name, dimension, value FROM daily_metrics WHERE metric_date = %s", (day,)
        ).fetchall()
    return {(name, dimension): value for name, dimension, value in rows}


def manual(action, day):
    response = httpx.post(f"{ANALYTICS}/{action}/{day}", timeout=60)
    return response.raise_for_status().json()


def archive(day: date):
    try:
        body = s3_client().get_object(Bucket=BUCKET, Key=f"daily/{day}/aggregates.json")["Body"]
        with body:
            return json.loads(body.read())
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            return None
        raise
