import os, time, json
from datetime import datetime, timezone

import boto3
import clickhouse_connect
from confluent_kafka import DeserializingConsumer, KafkaError
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

FLUSH_SECONDS = int(os.getenv("CONSUMER_FLUSH_SECONDS", "5"))

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

def utc_now():
    return datetime.now(timezone.utc)

def parse_event_time(s: str) -> datetime:
    """
    Accepts ISO-8601 strings like:
      2025-12-24T09:33:37.123456Z
      2025-12-24T09:33:37Z
      2025-12-24T09:33:37.123456+00:00
    Returns tz-aware UTC datetime.
    """
    if not s:
        return utc_now()

    s = str(s).strip()
    # normalize trailing Z to +00:00 for fromisoformat
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        # fallback: very permissive parse (seconds precision)
        # try: 2025-12-24 09:33:37
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            return utc_now()

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def ensure_bucket(s3):
    # create bucket if missing (minio is S3 compatible)
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
    except Exception:
        s3.create_bucket(Bucket=MINIO_BUCKET)

def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASS,
        region_name="us-east-1",
    )

    ensure_bucket(s3)

    ch = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_HTTP_PORT,
        username=CH_USER,
        password=CH_PASS,
        database=CH_DB,
    )

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
    print(f"[consumer] topic={TOPIC} -> minio://{MINIO_BUCKET} + clickhouse://{CH_DB}.events_raw")

    buffer = []
    last_flush = time.time()

    while True:
        try:
            msg = consumer.poll(1.0)
        except Exception as ex:
            # confluent_kafka can raise ConsumeError on UNKNOWN_TOPIC_OR_PART
            print("[consumer] poll exception:", ex)
            time.sleep(2)
            continue

        if msg is None:
            pass
        elif msg.error():
            # don't crash on transient errors
            if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                print(f"[consumer] topic not ready yet: {TOPIC} (waiting...)")
                time.sleep(2)
                continue
            print("[consumer] error:", msg.error())
        else:
            ev = msg.value()
            if ev:
                buffer.append(ev)

        if buffer and (time.time() - last_flush >= FLUSH_SECONDS):
            flush(buffer, s3, ch)
            buffer.clear()
            last_flush = time.time()

def flush(events, s3, ch):
    now = utc_now()
    dt = now.strftime("%Y-%m-%d")
    hh = now.strftime("%H")
    key = f"dt={dt}/hour={hh}/events_{now.strftime('%Y%m%d_%H%M%S')}.jsonl"

    body = "\n".join(json.dumps(e, ensure_ascii=False) for e in events) + "\n"
    s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=body.encode("utf-8"))
    print(f"[consumer] minio wrote: s3://{MINIO_BUCKET}/{key} ({len(events)} events)")

    rows = []
    for e in events:
        rows.append([
            str(e.get("event_id", "")),
            parse_event_time(e.get("event_time")),
            str(e.get("type", "")),
            int(e.get("user_id", 0)),
            float(e.get("amount", 0.0)),
            str(e.get("currency", "")),
            json.dumps(e.get("meta") or {}, ensure_ascii=False),
            utc_now(),  # ingested_at (اگر جدول شما این ستون را دارد)
        ])

    # توجه: اسم ستون شما meta_json است (نه meta_json اشتباه قبلی) و ingested_at هم باید insert شود
    ch.insert(
        "events_raw",
        rows,
        column_names=[
            "event_id", "event_time", "type", "user_id", "amount", "currency", "meta_json", "ingested_at"
        ],
    )
    print(f"[consumer] clickhouse inserted: {len(rows)} rows")

if __name__ == "__main__":
    main()
