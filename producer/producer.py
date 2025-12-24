import os, time, json, requests
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC = os.getenv("KAFKA_TOPIC", "events.v1")
API_URL = os.getenv("API_URL", "http://mock-api:8000/event")
POLL_SECONDS = float(os.getenv("PRODUCER_POLL_SECONDS", "1"))

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

def to_avro(obj, ctx):
    # meta values must be strings for our schema
    meta = obj.get("meta") or {}
    meta = {str(k): str(v) for k, v in meta.items()}
    return {
        "event_id": obj["event_id"],
        "event_time": obj["event_time"],
        "type": obj["type"],
        "user_id": int(obj["user_id"]),
        "amount": float(obj["amount"]),
        "currency": obj["currency"],
        "meta": meta,
    }

def main():
    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(sr, avro_schema_str, to_avro)

    producer = SerializingProducer({
        "bootstrap.servers": KAFKA_BROKERS,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
        "linger.ms": 50,
        "acks": "all",
    })

    print(f"[producer] topic={TOPIC} api={API_URL} brokers={KAFKA_BROKERS} sr={SCHEMA_REGISTRY_URL}")

    while True:
        r = requests.get(API_URL, timeout=5)
        r.raise_for_status()
        event = r.json()

        producer.produce(topic=TOPIC, key=event["event_id"], value=event)
        producer.poll(0)
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
