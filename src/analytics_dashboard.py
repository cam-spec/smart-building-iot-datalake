import pandas as pd
from pymongo import MongoClient
import streamlit as st
from datetime import datetime, timedelta, timezone


# ---------- Mongo helpers ----------

def get_mongo_client():
    return MongoClient("mongodb://localhost:27017/")


@st.cache_data(ttl=30)
def load_sensor_data(limit: int = 5000):
    client = get_mongo_client()
    db = client["smart_building"]
    coll = db["sensor_readings"]

    cursor = (
        coll.find({})
        .sort("ts", -1)          # newest first
        .limit(limit)
    )

    docs = list(cursor)
    if not docs:
        return pd.DataFrame()

    # Flatten documents into a DataFrame
    rows = []
    for d in docs:
        meta = d.get("metadata", {})
        metrics = d.get("metrics", {})
        rows.append(
            {
                "ts": d.get("ts"),
                "event_id": meta.get("event_id"),
                "sensor_id": meta.get("sensor_id"),
                "building_id": meta.get("building_id"),
                "floor": meta.get("floor"),
                "room": meta.get("room"),
                "temperature_c": metrics.get("temperature_c"),
                "occupancy_count": metrics.get("occupancy_count"),
                "energy_w": metrics.get("energy_w"),
            }
        )

    df = pd.DataFrame(rows)

    # --- FIX: normalise timestamps then parse as UTC ---
    from datetime import datetime as _dt

    # Convert any datetime / object to ISO string first
    df["ts"] = df["ts"].apply(
        lambda v: v.isoformat() if isinstance(v, _dt) else str(v)
    )
    # Then let pandas parse them all as tz-aware UTC
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")

    # Ensure numeric types for charts (defensive)
    for col in ["temperature_c", "occupancy_count", "energy_w"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Sort oldest → newest for charts
    df = df.sort_values("ts")
    return df


@st.cache_data(ttl=30)
def load_deadletter_summary(limit: int = 50):
    client = get_mongo_client()
    db = client["smart_building"]
    coll = db["dead_letter"]

    total = coll.count_documents({})
    recent = list(
        coll.find({})
        .sort("ts", -1)
        .limit(limit)
    )

    rows = []
    for d in recent:
        rows.append(
            {
                "ts": d.get("ts"),
                "errors": "; ".join(d.get("errors", [])),
                "raw_building": d.get("raw", {}).get("building_id"),
                "raw_room": d.get("raw", {}).get("room"),
            }
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    return total, df


# ---------- Streamlit UI ----------

def main():
    st.set_page_config(
        page_title="Smart Building IoT – Analytics",
        layout="wide",
    )

    st.title("Smart Building IoT – Analytics Dashboard")

    st.markdown(
        "Real-time view of temperature, occupancy and energy metrics "
        "streaming from the Kafka → MongoDB pipeline."
    )

    # Load data
    df = load_sensor_data(limit=5000)
    total_dead, df_dead = load_deadletter_summary()

    if df.empty:
        st.warning("No sensor data found yet. Start the producer & consumer.")
        return

    # Sidebar filters
    st.sidebar.header("Filters")

    buildings = sorted([b for b in df["building_id"].dropna().unique()])
    selected_building = st.sidebar.selectbox(
        "Building", options=["(All)"] + buildings
    )

    floors = sorted([int(f) for f in df["floor"].dropna().unique()])
    selected_floor = st.sidebar.selectbox(
        "Floor", options=["(All)"] + floors
    )

    rooms = sorted([r for r in df["room"].dropna().unique()])
    selected_room = st.sidebar.selectbox(
        "Room", options=["(All)"] + rooms
    )

    # Simple time window: last X hours
    hours_options = [1, 6, 12, 24]
    selected_hours = st.sidebar.selectbox(
        "Time window (hours, approx.)", options=hours_options, index=3
    )

    # Timezone-aware cutoff in UTC to match df["ts"]
    time_cutoff = datetime.now(timezone.utc) - timedelta(hours=selected_hours)
    mask = df["ts"] >= time_cutoff

    if selected_building != "(All)":
        mask &= df["building_id"] == selected_building
    if selected_floor != "(All)":
        mask &= df["floor"] == selected_floor
    if selected_room != "(All)":
        mask &= df["room"] == selected_room

    df_filtered = df[mask]

    if df_filtered.empty:
        st.warning("No data for the selected filters/time window.")
        return

    # ---------- KPI cards ----------
    latest = df_filtered.iloc[-1]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric(
        "Temperature (°C)",
        f"{latest['temperature_c']:.2f}" if latest["temperature_c"] is not None else "N/A",
    )
    col2.metric(
        "Occupancy (people)",
        f"{latest['occupancy_count']}" if latest["occupancy_count"] is not None else "N/A",
    )
    col3.metric(
        "Power (W)",
        f"{latest['energy_w']:.2f}" if latest["energy_w"] is not None else "N/A",
    )
    col4.metric(
        "Dead-letter events (total)",
        f"{total_dead}",
    )

    st.markdown("---")

    # ---------- Charts ----------
    left, right = st.columns(2)

    with left:
        st.subheader("Temperature Over Time")
        temp_series = df_filtered[["ts", "temperature_c"]].set_index("ts")
        st.line_chart(temp_series)

        st.subheader("Occupancy Over Time")
        occ_series = df_filtered[["ts", "occupancy_count"]].set_index("ts")
        st.line_chart(occ_series)

    with right:
        st.subheader("Energy Usage Over Time")
        energy_series = df_filtered[["ts", "energy_w"]].set_index("ts")
        st.line_chart(energy_series)

        st.subheader("Raw Events (latest)")
        st.dataframe(
            df_filtered.sort_values("ts", ascending=False).head(20),
            use_container_width=True,
        )

    st.markdown("---")

    # ---------- Dead-letter section ----------
    st.subheader("Dead-letter Events (Reliability View)")
    st.caption(
        "Events that failed validation in the Kafka consumer. "
        "Useful to understand data quality issues."
    )

    if df_dead.empty:
        st.write("No dead-letter events recorded yet.")
    else:
        st.dataframe(df_dead.head(50), use_container_width=True)


if __name__ == "__main__":
    main()
