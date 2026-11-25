CREATE DATABASE IF NOT EXISTS cinema;

CREATE TABLE IF NOT EXISTS cinema.kafka_events
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8('VIEW_STARTED' = 0, 'VIEW_FINISHED' = 1, 'VIEW_PAUSED' = 2,
                     'VIEW_RESUMED' = 3, 'LIKED' = 4, 'SEARCHED' = 5),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE' = 0, 'DESKTOP' = 1, 'TV' = 2, 'TABLET' = 3),
    session_id String,
    progress_seconds Int32
)
ENGINE = Kafka()
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
         kafka_topic_list = 'movie-events',
         kafka_group_name = 'cinema-clickhouse-v1',
         kafka_format = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://schema-registry:8081',
         kafka_num_consumers = 1,
         kafka_flush_interval_ms = 1000;

CREATE TABLE IF NOT EXISTS cinema.raw_events
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8('VIEW_STARTED' = 0, 'VIEW_FINISHED' = 1, 'VIEW_PAUSED' = 2,
                     'VIEW_RESUMED' = 3, 'LIKED' = 4, 'SEARCHED' = 5),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE' = 0, 'DESKTOP' = 1, 'TV' = 2, 'TABLET' = 3),
    session_id String,
    progress_seconds Int32,
    event_date Date MATERIALIZED toDate(timestamp, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id, event_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.consume_events TO cinema.raw_events AS
SELECT event_id, user_id, movie_id, event_type, timestamp, device_type, session_id, progress_seconds
FROM cinema.kafka_events;

CREATE VIEW IF NOT EXISTS cinema.events AS
SELECT event_id, user_id, movie_id, event_type, timestamp, device_type, session_id, progress_seconds, event_date
FROM cinema.raw_events FINAL;

CREATE TABLE IF NOT EXISTS cinema.daily_metrics
(
    metric_date Date,
    event_count UInt64,
    dau UInt64,
    avg_watch_seconds Nullable(Float64),
    conversion Nullable(Float64),
    cohort_size UInt64,
    retention Array(Nullable(Float64)),
    movie_ids Array(String),
    movie_views Array(UInt64),
    computed_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY metric_date;

CREATE USER IF NOT EXISTS dashboard IDENTIFIED WITH sha256_password BY 'dashboard_demo';
GRANT SELECT ON cinema.daily_metrics TO dashboard;
