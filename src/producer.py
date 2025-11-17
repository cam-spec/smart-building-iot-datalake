import json
import time
import uuid
import random
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer


# =========================
# CONFIG
# =========================

# Buildings, floors and rooms to simulate
BUILDINGS = ["B1", "B2", "B3", "B4"]
FLOORS = [1, 2, 3, 4]
ROOM_SUFFIXES = ["01", "02", "03", "04", "05"]  # e.g. rooms 101, 102, 203, 405

# Fault injection rate:
#   0.0  -> completely clean data (for Laith's analytics)
#   0.10 -> ~10% faulty messages (for reliability testing)
FAULT_INJECTION_RATE = 0.0

# Delay between events in seconds
EVENT_DELAY_SECONDS = 0.2


# =========================
# TIME HELPERS
# =========================

def now_iso() -> str:
    """Return current timestamp in ISO 8601 UTC format."""
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def random_timestamp() -> str:
    """Return a random timestamp within the last 24 hours."""
    offset_minutes = random.randint(0, 1440)
    ts = datetime.now(timezone.utc) - timedelta(minutes=offset_minutes)
    return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")


# =========================
# BUILD ALL ROOM COMBINATIONS
# =========================

# Example:
#   B1, floor 2, suffix "03" -> room "203"
ALL_COMBINATIONS = [
    (building, floor, f"{floor}{suffix}")
    for building in BUILDINGS
    for floor in FLOORS
    for suffix in ROOM_SUFFIXES
]

combo_index = 0  # used to cycle through combinations one by one


# =========================
# EVENT GENERATION
# =========================

def build_base_event() -> dict:
    """
    Build a clean (valid) sensor event, cycling through
    all building/floor/room combinations in a round-robin fashion.
    """
    global combo_index

    building, floor, room = ALL_COMBINATIONS[combo_index]
    combo_index = (combo_index + 1) % len(ALL_COMBINATIONS)

    event = {
        "event_id": str(uuid.uuid4()),
        "device_ts": random_timestamp(),
        "sensor_id": f"temp-{building}-F{floor}-R{room}",
        "building_id": building,
        "floor": floor,
        "room": room,
        "metrics": {
            "temperature_c": round(random.uniform(19.0, 25.0), 2),
            "occupancy_count": random.randint(0, 40),
            "energy_w": round(random.uniform(100.0, 2000.0), 2),
        },
    }
    return event


def apply_fault_injection(event: dict) -> dict:
    """
    Optionally corrupt an event for fault-tolerance testing.

    When FAULT_INJECTION_RATE = 0.0, this returns the event unchanged.
    """
    if random.random() >= FAULT_INJECTION_RATE:
        # No fault applied
        return event

    faulty = json.loads(json.dumps(event))  # deep copy

    faults = ["missing_field", "bad_type", "negative_value", "future_timestamp"]
    fault_type = random.choice(faults)

    if fault_type == "missing_field":
        # Drop the 'room' field
        faulty.pop("room", None)

    elif fault_type == "bad_type":
        # Make temperature a string instead of float
        faulty["metrics"]["temperature_c"] = "very hot"

    elif fault_type == "negative_value":
        # Make occupancy negative
        faulty["metrics"]["occupancy_count"] = -3

    elif fault_type == "future_timestamp":
        # Push timestamp 10 minutes into the future
        future_ts = datetime.now(timezone.utc) + timedelta(minutes=10)
        faulty["device_ts"] = (
            future_ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        )

    return faulty


def build_event() -> dict:
    """Generate either a clean or a faulty event, depending on FAULT_INJECTION_RATE."""
    base_event = build_base_event()
    return apply_fault_injection(base_event)


# =========================
# MAIN LOOP
# =========================

def main():
    print(
        "=== PRODUCER STARTED "
        "(B1–B4, Floors 1–4, Rooms 01–05, "
        f"fault_rate={FAULT_INJECTION_RATE}) ==="
    )

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

    try:
        while True:
            event = build_event()

            # Use building_id as the Kafka key so all events
            # from the same building go to the same partition
            key_bytes = event["building_id"].encode("utf-8")

            producer.send("building_sensors", key=key_bytes, value=event)
            print("[PRODUCER] Sent:", event)

            producer.flush()
            time.sleep(EVENT_DELAY_SECONDS)

    except KeyboardInterrupt:
        print("Stopping producer...")

    finally:
        producer.flush()
        producer.close()
        print("Producer stopped.")


if __name__ == "__main__":
    main()
