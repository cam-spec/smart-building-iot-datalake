
import uuid
import random
import json
from datetime import datetime, timezone

# Day 1: no Kafka import yet; we'll add it on Day 3.
# from kafka import KafkaProducer

def now_iso() -> str:
    """Return current UTC time in ISO-8601 format with milliseconds."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def build_event(building_id: str = "B1", floor: int = 2, room: str = "201") -> dict:
    """Build one synthetic sensor event following docs/event_schema.json."""
    return {
        "event_id": str(uuid.uuid4()),
        "device_ts": now_iso(),
        "sensor_id": f"temp-{building_id}-{floor}F-{room}",
        "building_id": building_id,
        "floor": floor,
        "room": room,
        "metrics": {
            "temperature_c": round(random.uniform(18, 28), 2),
            "occupancy_count": random.randint(0, 10),
            "energy_w": round(random.uniform(100, 500), 2),
        },
    }

def main():
    # Day 1: just demonstrate the schema by printing a sample event.
    event = build_event()
    print(json.dumps(event, indent=2))

if __name__ == "__main__":
    main()
