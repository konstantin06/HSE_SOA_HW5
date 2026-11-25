import io
import json
import os
import struct
from concurrent.futures import Future
from threading import Event, Thread

import httpx
from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from fastavro import parse_schema, schemaless_writer
from fastavro.validation import ValidationError, validate

from app.common import ROOT, log, retry

TOPIC = "movie-events"
SUBJECT = f"{TOPIC}-value"
VERSION = 1
BROKERS = os.environ.get("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
REGISTRY = os.environ.get("REGISTRY_URL", "http://schema-registry:8081")
SCHEMA = json.loads((ROOT / "schemas/movie-event-v1.avsc").read_text())


def prepare_topic(admin):
    topic = NewTopic(TOPIC, num_partitions=3, replication_factor=3, config={"min.insync.replicas": "2"})
    try:
        admin.create_topics([topic], request_timeout=12)[TOPIC].result()
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise


def register_schema():
    with httpx.Client(base_url=REGISTRY, timeout=8) as registry:
        subject = f"/subjects/{SUBJECT}/versions"
        body = {"schemaType": "AVRO", "schema": json.dumps(SCHEMA)}
        registry.put(f"/config/{SUBJECT}", json={"compatibility": "BACKWARD"}).raise_for_status()
        schema_id = registry.post(subject, json=body).raise_for_status().json()["id"]
        stored = registry.get(f"{subject}/{VERSION}").raise_for_status().json()
        if stored["id"] != schema_id:
            raise RuntimeError("Версия v1 в Registry отличается от локальной схемы")
        log.info("subject=%s version=%s schema_id=%s", SUBJECT, VERSION, schema_id)
        return schema_id


class Publisher:
    def __init__(self):
        self.admin = AdminClient({"bootstrap.servers": BROKERS})
        retry(lambda: prepare_topic(self.admin), KafkaException, attempts=5)
        self.schema_id = retry(register_schema, httpx.HTTPError, attempts=5)
        self.schema = parse_schema(SCHEMA)
        self.client = Producer({
            "bootstrap.servers": BROKERS,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 100,
            "retry.backoff.ms": 100,
            "retry.backoff.max.ms": 2000,
            "delivery.timeout.ms": 20000,
            "linger.ms": 2,
        })
        self.stop = Event()
        self.poller = Thread(target=self.poll, daemon=True)
        self.poller.start()

    def poll(self):
        while not self.stop.is_set():
            self.client.poll(0.05)

    def send(self, event):
        record = event.model_dump()
        record["event_id"] = str(event.event_id)
        try:
            validate(record, self.schema, strict=True)
        except ValidationError as exc:
            raise ValueError("Событие не соответствует Avro v1") from exc
        stream = io.BytesIO()
        stream.write(struct.pack(">BI", 0, self.schema_id))
        schemaless_writer(stream, self.schema, record, strict=True)
        value = stream.getvalue()
        delivered = Future()

        def confirm(error, message):
            if error is None:
                delivered.set_result((message.partition(), message.offset()))
            else:
                delivered.set_exception(KafkaException(error))

        def enqueue():
            self.client.produce(TOPIC, key=event.user_id.encode(), value=value, on_delivery=confirm)

        try:
            retry(enqueue, BufferError)
            partition, offset = delivered.result(timeout=22)
        except (KafkaException, BufferError, TimeoutError) as exc:
            raise ConnectionError("Нет подтверждения записи в Kafka") from exc
        log.info("published event_id=%s event_type=%s timestamp=%s partition=%d offset=%d",
                 event.event_id, event.event_type, event.timestamp.isoformat(), partition, offset)

    def ready(self):
        try:
            info = self.admin.list_topics(TOPIC, timeout=3).topics[TOPIC]
        except KafkaException:
            log.exception("Kafka metadata unavailable")
            return False
        return info.error is None and len(info.partitions) >= 3 and all(p.leader >= 0 for p in info.partitions.values())

    def close(self):
        self.stop.set()
        self.poller.join()
        self.client.flush(5)
