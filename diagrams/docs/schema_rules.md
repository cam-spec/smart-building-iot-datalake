# Event Schema Rules (building_sensors topic)

## Fields
- `event_id`: UUIDv4, non-empty.
- `device_ts`: ISO-8601 UTC timestamp from device.
- `sensor_id`: non-empty string, e.g. "temp-B1-2F-201".
- `building_id`: non-empty string, e.g. "B1".
- `floor`: integer (e.g. -2..100).
- `room`: non-empty string, e.g. "201".

## Metrics
- `metrics.temperature_c`: float, allowed range [-20, 60].
- `metrics.occupancy_count`: integer, allowed range [0, 500].
- `metrics.energy_w`: float, allowed range [0, 100000].

## Kafka topic configuration
- Topic name: `building_sensors`
- Message key: `building_id` (used for partitioning).
- Partitions (lab): 3 (design scales with number of buildings).
- Replication (conceptual design): 3.
