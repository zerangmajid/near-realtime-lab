
from fastapi import FastAPI
from datetime import datetime, timezone
import random, uuid

app = FastAPI()

CURRENCIES = ["USD", "EUR", "GBP", "AED", "IRR"]
TYPES = ["payment", "order", "shipment", "inventory"]

@app.get("/event")
def get_event():
    now = datetime.now(timezone.utc).isoformat()
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": now,
        "type": random.choice(TYPES),
        "user_id": random.randint(1, 1000),
        "amount": round(random.random() * 1000, 2),
        "currency": random.choice(CURRENCIES),
        "meta": {"source": "mock-api", "rand": random.randint(1, 999999)}
    }
