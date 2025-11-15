import json
import time
import uuid
import random
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer


# Returns current timestamp in ISO 8601 UTC format
def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


# Generates a random timestamp within the last 24 hours
def random_timestamp():
    offset_minutes = random.randint(0, 1440)
    ts = datetime.now(timezone.utc) - timedelta(minutes=offset_minutes)
    return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")


# Fixed allowed buildings and floors
BUILDINGS = ["B1", "B2", "B3", "B4"]
FLOORS = [1, 2, 3, 4]
ROOM_SUFFIXES = ["01", "02", "03", "04", "05"]


# Build ALL combinations so no building-floor-room is ever missing
ALL_COMBINATIONS = [
    (b, f, f"{f}{suffix}")   # Example room format: floor=2, suffix=03 → "203"
    for b in BUILDINGS
    for f in FLOORS
    for suffix in ROOM_SUFFIXES
]

# Used to cycle through combinations one-by-one
combo_index = 0


# Builds a single sensor event, cycling through ALL_COMBINATIONS
def build_event():
    global combo_index

    # Pick the next combination and loop when reaching the end
    building, floor, room = ALL_COMBINATIONS[combo_index]
    combo_index = (combo_index + 1) % len(ALL_COMBINATIONS)

    event = {
        "event_id": str(uuid.uuid4()),
        "device_ts": random_timestamp(),
        "sensor_id": f"temp-{building}-{floor}F-{room}",
        "building_id": building,
        "floor": floor,
        "room": room,
        "metrics": {
            "temperature_c": round(random.uniform(10.0, 35.0), 2),
            "occupancy_count": random.randint(0, 50),
            "energy_w": round(random.uniform(10.0, 2000.0), 2),
        },
    }

    return event


# Optional: fault injection disabled by default
def apply_fault_injection(event):
    return event


def main():
    print("=== PRODUCER STARTED (B1–B4, Floors 1–4, Rooms 01–05) ===")

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

    try:
        while True:
            event = build_event()
            event = apply_fault_injection(event)
            producer.send("building_sensors", value=event)
            print("[PRODUCER] Sent:", event)
            time.sleep(0.2)   # Faster cycling, adjust if needed
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()
        print("Producer stopped.")


if __name__ == "__main__":
    main()
