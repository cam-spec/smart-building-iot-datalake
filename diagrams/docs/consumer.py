import json
from typing import Tuple, Dict, Any

REQUIRED_FIELDS = ["event_id", "device_ts", "sensor_id",
                   "building_id", "floor", "room", "metrics"]

def validate_event(ev: Dict[str, Any]) -> Tuple[bool, str]:
    """Basic validation according to docs/schema_rules.md."""
    for field in REQUIRED_FIELDS:
        if field not in ev:
            return False, f"MISSING_FIELD:{field}"

    m = ev["metrics"]
    if not (-20 <= m.get("temperature_c", 999) <= 60):
        return False, "TEMP_OUT_OF_RANGE"
    if m.get("occupancy_count", -1) < 0:
        return False, "OCC_NEGATIVE"
    if m.get("energy_w", -1) < 0:
        return False, "ENERGY_NEGATIVE"

    return True, "OK"

def main():
    # Day 1: simple test using the example JSON file.
    with open("docs/event_schema.json", "r") as f:
        example = json.load(f)

    ok, reason = validate_event(example)
    print("VALIDATION RESULT:", ok, reason)

if __name__ == "__main__":
    main()
