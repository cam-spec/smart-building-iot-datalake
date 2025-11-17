import json
from datetime import datetime, timezone, timedelta

from kafka import KafkaConsumer
from pymongo import MongoClient
from dateutil.parser import isoparse   # proper ISO timestamp parser


def now_iso():
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def validate_event(event: dict):
    errors = []

    # Required fields
    required = [
        "event_id",
        "device_ts",
        "sensor_id",
        "building_id",
        "floor",
        "room",
        "metrics",
    ]

    for field in required:
        if field not in event:
            errors.append(f"Missing field: {field}")

    # If core fields are missing, no point doing deeper checks
    if errors:
        return errors

    # Metrics checks
    metrics = event.get("metrics", {})

    temp = metrics.get("temperature_c")
    occ = metrics.get("occupancy_count")
    energy = metrics.get("energy_w")

    # Temperature
    if temp is not None:
        if not isinstance(temp, (int, float)):
            errors.append("temperature_c must be numeric")
        elif not (-20 <= temp <= 60):
            errors.append("temperature_c out of valid range (-20 to 60)")

    # Occupancy
    if occ is not None:
        if not isinstance(occ, int):
            errors.append("occupancy_count must be integer")
        else:
            if occ < 0:
                errors.append("occupancy_count cannot be negative")

    # Energy
    if energy is not None:
        if not isinstance(energy, (int, float)):
            errors.append("energy_w must be numeric")
        elif energy < 0:
            errors.append("energy_w cannot be negative")

    # Timestamp validation (with small clock skew allowance)
    try:
        ts = isoparse(event["device_ts"])  # accepts ISO with Z + millis
        if ts > datetime.now(timezone.utc) + timedelta(minutes=5):
            errors.append("device_ts is more than 5 minutes in the future")
    except Exception:
        errors.append("Invalid device_ts format")

    return errors


def main():
    consumer = KafkaConsumer(
        "building_sensors",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="smart-building-consumer-v1",
    )

    mongo = MongoClient("mongodb://localhost:27017/")
    db = mongo["smart_building"]
    readings = db["sensor_readings"]
    dead = db["dead_letter"]

    print("Consumer with validation + dead-letter started. Ctrl+C to stop.")

    try:
        for msg in consumer:
            event = msg.value
            errors = validate_event(event)

            # If failed validation → send to dead-letter
            if errors:
                print("[DEAD LETTER]", errors)
                dead.insert_one(
                    {
                        "ts": now_iso(),
                        "errors": errors,
                        "raw": event,
                    }
                )
                continue

            # Store ts as a real datetime in Mongo (better for time-series)
            ts_parsed = isoparse(event["device_ts"])

            doc = {
                "ts": ts_parsed,
                "metadata": {
                    "event_id": event["event_id"],
                    "sensor_id": event["sensor_id"],
                    "building_id": event["building_id"],
                    "floor": event["floor"],
                    "room": event["room"],
                },
                "metrics": event["metrics"],
            }

            readings.insert_one(doc)
            print(
                f"[CONSUMER] Inserted → "
                f"{event['building_id']} F{event['floor']} {event['room']}"
            )

    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        print("Consumer closed.")


if __name__ == "__main__":
    main()
