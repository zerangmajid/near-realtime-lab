import os, time, json
from datetime import datetime, timezone

import boto3
import clickhouse_connect
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC = os.getenv("KAFKA_TOPIC", "events.v1")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw-events")

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv("CLICKHOUSE_DB", "analytics")
CH_TABLE = os.getenv("CLICKHOUSE_TABLE", "events_raw")  # فقط نام جدول
CH_FULL_TABLE = f"{CH_DB}.{CH_TABLE}"  # analytics.events_raw

FLUSH_SECONDS = int(os.getenv("CONSUMER_FLUSH_SECONDS", "5"))

# Same schema as producer (Deserializer will fetch writer schema via SR)
avro_schema_str = """
{
  "type":"record",
  "name":"Event",
  "namespace":"lab",
  "fields":[
    {"name":"event_id","type":"string"},
    {"name":"event_time","type":"string"},
    {"name":"type","type":"string"},
    {"name":"user_id","type":"int"},
    {"name":"amount","type":"double"},
    {"name":"currency","type":"string"},
    {"name":"meta","type":{"type":"map","values":"string"}}
  ]
}
"""

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def parse_event_time(value) -> datetime:
    """
    Convert incoming event_time (usually ISO string) to timezone-aware UTC datetime
    for ClickHouse DateTime64.
    Accepts:
      - ISO string like '2025-12-24T08:55:24.123Z'
      - ISO string with offset '2025-12-24T08:55:24+00:00'
      - datetime (returns as UTC)
      - unix timestamp (int/float, seconds)
    """
    if value is None:
        return utc_now()

    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)

    if isinstance(value, str):
        s = value.strip()
        # handle Z
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            # fallback: if format is weird, just use now
            return utc_now()

    return utc_now()

def ensure_bucket(s3):
    # create bucket if not exists (MinIO supports this style)
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
    except Exception:
        # If not exists or access denied, try to create
        s3.create_bucket(Bucket=MINIO_BUCKET)

def main():
    # MinIO client (S3)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASS,
        region_name="us-east-1",
    )

    ensure_bucket(s3)

    # ClickHouse client (default db set to analytics)
    ch = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_HTTP_PORT,
        username=CH_USER,
        password=CH_PASS,
        database=CH_DB,
    )

    # Schema Registry + Kafka consumer
    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(sr, avro_schema_str)

    consumer = DeserializingConsumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": "lab-consumer-1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": avro_deserializer,
    })

    consumer.subscribe([TOPIC])

    print(f"[consumer] topic={TOPIC} -> minio://{MINIO_BUCKET} + clickhouse://{CH_FULL_TABLE}")

    buffer = []
    last_flush = time.time()

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            pass
        elif msg.error():
            # مهم: این همون UNKNOWN_TOPIC_OR_PART میشه اگر topic وجود نداشته باشه
            print("[consumer] error:", msg.error())
        else:
            ev = msg.value()
            buffer.append(ev)

        if buffer and (time.time() - last_flush >= FLUSH_SECONDS):
            flush(buffer, s3, ch)
            buffer.clear()
            last_flush = time.time()

def flush(events, s3, ch):
    # 1) Write to MinIO as JSONL
    now = utc_now()
    dt = now.strftime("%Y-%m-%d")
    hh = now.strftime("%H")
    key = f"dt={dt}/hour={hh}/events_{now.strftime('%Y%m%d_%H%M%S')}.jsonl"
    body = "\n".join(json.dumps(e, ensure_ascii=False) for e in events) + "\n"

    s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=body.encode("utf-8"))
    print(f"[consumer] minio wrote: s3://{MINIO_BUCKET}/{key} ({len(events)} events)")

    # 2) Insert into ClickHouse
    rows = []
    bad = 0

    for e in events:
        try:
            event_time_dt = parse_event_time(e.get("event_time"))
            if not isinstance(event_time_dt, datetime):
                raise ValueError("parse_event_time did not return datetime")
        except Exception:
            bad += 1
            event_time_dt = utc_now()

        rows.append([
            str(e.get("event_id", "")),
            event_time_dt,                 # ✅ datetime
            str(e.get("type", "")),
            int(e.get("user_id", 0)),
            float(e.get("amount", 0.0)),
            str(e.get("currency", "")),
            json.dumps(e.get("meta") or {}, ensure_ascii=False),
            utc_now(),                     # ✅ ingested_at datetime
        ])

    ch.insert(CH_TABLE, rows, column_names=[
        "event_id", "event_time", "type", "user_id", "amount", "currency", "meta_json", "ingested_at"
    ])

    print(f"[consumer] clickhouse inserted: {len(rows)} rows (bad event_time fixed: {bad})")

if __name__ == "__main__":
    main()
