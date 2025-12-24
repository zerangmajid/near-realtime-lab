CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.events_raw
(
  event_id String,
  event_time DateTime64(6, 'UTC'),
  type String,
  user_id UInt32,
  amount Float64,
  currency String,
  meta_json String,
  ingested_at DateTime64(6, 'UTC')
)
ENGINE = MergeTree
ORDER BY (event_time, event_id);
