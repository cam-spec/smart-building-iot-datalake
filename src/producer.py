import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_event():
    """Base clean event (before fault injection)."""
    return {
        "event_id": str(uuid.uuid4()),
        "device_ts": now_iso(),
        "sensor_id": "temp-B1-2F-201",
        "building_id": "B1",
        "floor": 2,
        "room": "201",
        "metrics": {
            "temperature_c": round(random.uniform(20.0, 27.0), 2),
            "occupancy_count": random.randint(0, 5),
            "energy_w": round(random.uniform(100.0, 250.0), 2),
        },
    }


def apply_fault_injection(event: dict) -> dict:
    """Inject random faults into an event (5% probability each)."""

    # Remove required field
    if random.random() < 0.05:
        event.pop("room", None)

    # Negative occupancy
    if random.random() < 0.05:
        event["metrics"]["occupancy_count"] = -3

    # Bad data type
    if random.random() < 0.05:
        event["metrics"]["temperature_c"] = "BAD_VALUE"

    # Future timestamp
    if random.random() < 0.05:
        event["device_ts"] = "3025-01-01T00:00:00Z"

    return event


def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5
    )

    print("Producer with fault injection started. Ctrl+C to stop.")

    try:
        while True:
            event = build_event()
            event = apply_fault_injection(event)

            producer.send("building_sensors", value=event)
            print(f"[PRODUCER] Sent event → {event}")
            time.sleep(1)

    except KeyboardInterrupt:
        print("Stopping...")

    finally:
        producer.flush()
        producer.close()
        print("Producer closed.")


if __name__ == "__main__":
    main()
