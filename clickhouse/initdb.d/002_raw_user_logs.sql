CREATE TABLE IF NOT EXISTS log_beaver.raw_user_logs
(
    event_id String,
    timestamp DateTime('UTC'),
    event_type LowCardinality(String),
    user_id UInt32,
    user_gender LowCardinality(String),
    user_age_group LowCardinality(String),
    device_type LowCardinality(String),
    item_id String,
    item_category LowCardinality(String),
    item_price Float64,
    session_id String,
    page_url String,
    referrer LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (event_type, timestamp)
TTL timestamp + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
