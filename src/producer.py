import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


def now_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format with milliseconds."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_event() -> dict:
    """Construct one synthetic IoT sensor event."""
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


def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        acks="all",
    )

    print("Kafka producer created. Sending events to 'building_sensors'... (Ctrl+C to stop)")

    try:
        while True:
            event = build_event()
            producer.send("building_sensors", value=event)
            print(f"[PRODUCER] Sent event for {event['building_id']} {event['room']} at {event['device_ts']}")
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.flush()
        producer.close()
        print("Producer closed.")


if __name__ == "__main__":
    main()
