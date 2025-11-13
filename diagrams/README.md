# Smart-Building IoT Data Lake (Kafka + MongoDB)

## Problem
We simulate IoT sensors in a smart building that emit temperature, occupancy, and energy readings in real time.  
We need a scalable, fault-tolerant pipeline to ingest, validate, and store these events for monitoring and analytics.

## Architecture (High-Level)
Sensors → **Apache Kafka** (`building_sensors` topic, key = `building_id`)  
→ **Python Consumer** (validate, enrich with server timestamp, retry logic, dead-letter)  
→ **MongoDB** time-series collection (`sensor_readings`) + `dead_letter` for invalid events  
→ **Streamlit / BI Dashboard** for monitoring.

## Event Schema
Each Kafka message is a JSON document with:
- IDs and context: `event_id`, `device_ts`, `sensor_id`, `building_id`, `floor`, `room`
- Metrics: `temperature_c`, `occupancy_count`, `energy_w`

Example JSON is in `docs/event_schema.json`. Validation rules are in `docs/schema_rules.md`.

## Implementation Plan
- Day 1: Architecture, schemas, code skeletons 
- Day 2: Stand up Kafka + MongoDB using Docker.
- Day 3: Implement producer (simulated sensors → Kafka).
- Day 4: Implement consumer (Kafka → MongoDB + dead-letter).
- Day 5: Measure latency/throughput, capture screenshots.
- Day 6–7: Write report and prepare slides.
