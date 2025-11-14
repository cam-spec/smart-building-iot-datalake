import json
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Tuple

from kafka import KafkaConsumer
from pymongo import MongoClient


def now_iso() -> str:
    """Return current UTC time in the same ISO format as the events."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def validate_event(event: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Apply the schema + business rules.
    Returns (is_valid, reason_if_invalid).
    """

    # ---------- Required fields ----------
    required_fields = [
        "event_id",
        "device_ts",
        "sensor_id",
        "building_id",
        "floor",
        "room",
        "metrics",
    ]

    for field in required_fields:
        if field not in event:
            return False, f"Missing field: {field}"

    # ---------- Basic type checks ----------
    if not isinstance(event["event_id"], str) or not event["event_id"]:
        return False, "event_id must be non-empty string"

    # device_ts: ISO-8601, not > 5 min in future
    try:
        device_dt = datetime.fromisoformat(event["device_ts"].replace("Z", "+00:00"))
    except Exception:
        return False, "device_ts is not valid ISO-8601"

    now = datetime.now(timezone.utc)
    if device_dt - now > timedelta(minutes=5):
        return False, "device_ts is more than 5 minutes in the future"

    # Location checks
    if not isinstance(event["sensor_id"], str) or not event["sensor_id"]:
        return False, "sensor_id must be non-empty string"

    if not isinstance(event["building_id"], str) or not event["building_id"]:
        return False, "building_id must be non-empty string"

    if not isinstance(event["room"], str) or not event["room"]:
        return False, "room must be non-empty string"

    if not isinstance(event["floor"], int):
        return False, "floor must be integer"

    # ---------- Metrics checks ----------
    metrics = event["metrics"]
    if not isinstance(metrics, dict):
        return False, "metrics must be an object"

    # temperature_c: float in [-20, 60]
    temp = metrics.get("temperature_c")
    if not isinstance(temp, (int, float)) or not (-20 <= temp <= 60):
        return False, "temperature_c out of range [-20, 60]"

    # occupancy_count: int >= 0
    occ = metrics.get("occupancy_count")
    if not isinstance(occ, int) or occ < 0:
        return False, "occupancy_count must be int >= 0"

    # energy_w: float >= 0
    energy = metrics.get("energy_w")
    if not isinstance(energy, (int, float)) or energy < 0:
        return False, "energy_w must be >= 0"

    return True, ""


def main() -> None:
    # ---------- MongoDB connection ----------
    mongo_client = MongoClient("mongodb://localhost:27017/")
    db = mongo_client["smart_building"]

    sensor_readings = db["sensor_readings"]
    dead_letter = db["dead_letter"]

    # ---------- Kafka consumer ----------
    consumer = KafkaConsumer(
        "building_sensors",
        bootstrap_servers="localhost:9092",
        group_id="smart-building-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    print("Consumer started. Listening to 'building_sensors'... (Ctrl+C to stop)")

    try:
        for message in consumer:
            event = message.value

            try:
                is_valid, reason = validate_event(event)

                if is_valid:
                    # Document shape for the time-series collection
                    doc = {
                        "ts": event["device_ts"],
                        "metadata": {
                            "event_id": event["event_id"],
                            "sensor_id": event["sensor_id"],
                            "building_id": event["building_id"],
                            "floor": event["floor"],
                            "room": event["room"],
                        },
                        "metrics": event["metrics"],
                    }
                    sensor_readings.insert_one(doc)
                    print(
                        f"[CONSUMER] Inserted sensor_reading for "
                        f"{event['building_id']} {event['room']}"
                    )
                else:
                    # Anything that fails validation goes to dead_letter
                    dead_doc = {
                        "ts": now_iso(),
                        "reason": reason,
                        "raw_message": event,
                    }
                    dead_letter.insert_one(dead_doc)
                    print(f"[CONSUMER] Sent event to dead_letter: {reason}")

            except Exception as e:
                # Any unexpected error also goes to dead_letter
                dead_doc = {
                    "ts": now_iso(),
                    "reason": f"Unhandled error: {e}",
                    "raw_message": event,
                }
                dead_letter.insert_one(dead_doc)
                print(f"[CONSUMER] ERROR -> sent to dead_letter: {e}")

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        consumer.close()
        mongo_client.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()
