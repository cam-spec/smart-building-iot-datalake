import pymongo
from datetime import datetime, timezone
import pandas as pd


# =============================
# Mongo Connection
# =============================

def get_mongo():
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client.smart_building
    return db


# =============================
# Main Aggregation Function
# =============================

def run_daily_aggregation():
    db = get_mongo()

    print("Running daily aggregation...")

    # Read all raw events
    cursor = db.sensor_readings.find({})

    events = list(cursor)
    if not events:
        print("No data found in sensor_readings.")
        return

    # Convert to DataFrame
    df = pd.DataFrame(events)

    # Flatten metadata + metrics fields
    df["building_id"] = df["metadata"].apply(lambda x: x["building_id"])
    df["floor"] = df["metadata"].apply(lambda x: x["floor"])
    df["room"] = df["metadata"].apply(lambda x: x["room"])

    df["temperature_c"] = df["metrics"].apply(lambda x: x["temperature_c"])
    df["occupancy_count"] = df["metrics"].apply(lambda x: x["occupancy_count"])
    df["energy_w"] = df["metrics"].apply(lambda x: x["energy_w"])

    # Convert ts to datetime
    df["ts"] = pd.to_datetime(df["ts"], utc=True)

    # Extract day (YYYY-MM-DD)
    df["day"] = df["ts"].dt.strftime("%Y-%m-%d")

    # Group by day + building + floor + room
    grouped = df.groupby(["day", "building_id", "floor", "room"]).agg({
        "temperature_c": ["mean", "min", "max"],
        "occupancy_count": ["mean", "min", "max"],
        "energy_w": ["mean", "min", "max"],
        "ts": "count"
    })

    # Flatten multi-index column names
    grouped.columns = [
        "_".join(col).strip() for col in grouped.columns.values
    ]

    grouped = grouped.reset_index()

    # Write to Mongo
    db.daily_summary.delete_many({})  # optional: clear old summaries

    summary_records = grouped.to_dict(orient="records")
    db.daily_summary.insert_many(summary_records)

    print(f"Daily summary written: {len(summary_records)} records.")


# =============================
# Entry Point
# =============================

if __name__ == "__main__":
    run_daily_aggregation()

