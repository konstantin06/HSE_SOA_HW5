CREATE TABLE IF NOT EXISTS daily_metrics (
    metric_date date NOT NULL,
    metric_name text NOT NULL,
    dimension text NOT NULL DEFAULT '',
    value double precision,
    computed_at timestamptz NOT NULL,
    PRIMARY KEY (metric_date, metric_name, dimension)
);

CREATE TABLE IF NOT EXISTS export_queue (
    metric_date date PRIMARY KEY,
    available_on date NOT NULL
);
