import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient


# ==================================
# CONFIG
# ==================================

BUILDINGS = ["B1", "B2", "B3", "B4"]
FLOORS = [1, 2, 3, 4]
ROOM_SUFFIXES = ["01", "02", "03", "04", "05"]

EVENTS_PER_ROOM_PER_DAY = 30   # 30 events x 640 rooms = ~19,200 events/day
DAYS = 7                       # generate for last 7 days


# ==================================
# MONGO CONNECTION
# ==================================

client = MongoClient("mongodb://localhost:27017")
db = client.smart_building
collection = db.sensor_readings


# ==================================
# EVENT BUILDER
# ==================================

def build_event(building, floor, room, timestamp):
    """Create one historical sensor event."""
    return {
        "event_id": str(uuid.uuid4()),
        "device_ts": timestamp.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
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


# ==================================
# MAIN GENERATOR
# ==================================

def main():
    print("Generating 7 days of historical IoT events...")

    total_inserted = 0

    for day_offset in range(DAYS):
        day_date = datetime.now(timezone.utc) - timedelta(days=day_offset + 1)

        print(f"Generating events for: {day_date.date()}")

        daily_events = []

        # generate events for each room
        for building in BUILDINGS:
            for floor in FLOORS:
                for suffix in ROOM_SUFFIXES:
                    room = f"{floor}{suffix}"

                    for i in range(EVENTS_PER_ROOM_PER_DAY):
                        # random timestamps spread across the day
                        ts = day_date + timedelta(
                            hours=random.randint(0, 23),
                            minutes=random.randint(0, 59),
                            seconds=random.randint(0, 59)
                        )

                        event = build_event(building, floor, room, ts)
                        daily_events.append(event)

        if daily_events:
            collection.insert_many(daily_events)
            total_inserted += len(daily_events)

    print(f"Inserted total historical events: {total_inserted}")


if __name__ == "__main__":
    main()
