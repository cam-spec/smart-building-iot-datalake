# MongoDB Time-Series Design

## Database and Collections
- Database: `smart_building`
- Time-series collection: `sensor_readings`
- Standard collection: `dead_letter`

## sensor_readings Document Shape
```json
{
  "ts": "2025-11-13T12:01:23.789Z",
  "device_ts": "2025-11-13T12:01:23.456Z",
  "metadata": {
    "event_id": "88b8f0f6-5f8e-4a9a-9b2a-0a9f0b2e9e1f",
    "sensor_id": "temp-B1-2F-201",
    "building_id": "B1",
    "floor": 2,
    "room": "201"
  },
  "metrics": {
    "temperature_c": 24.6,
    "occupancy_count": 3,
    "energy_w": 182.4
  }
}
