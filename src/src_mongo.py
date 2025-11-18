# src/setup_mongo.py
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["smart_building"]

# Create time-series collection if not exists
if "sensor_readings" not in db.list_collection_names():
    db.create_collection(
        "sensor_readings",
        timeseries={
            "timeField": "ts",
            "metaField": "metadata",
            "granularity": "seconds"
        }
    )

# Create dead_letter collection if not exists
if "dead_letter" not in db.list_collection_names():
    db.create_collection("dead_letter")

print("MongoDB setup complete.")
