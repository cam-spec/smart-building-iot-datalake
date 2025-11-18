import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# ------------------------------
# MongoDB Connection
# ------------------------------
@st.cache_data
def get_daily_data():
    client = MongoClient("mongodb://localhost:27017")
    db = client.smart_building
    data = list(db.daily_summary.find({}))
    return pd.DataFrame(data)


# ------------------------------
# Page Layout
# ------------------------------
def main():
    st.set_page_config(page_title="Daily Summary Analytics", layout="wide")

    st.title("Smart Building IoT – Daily Summary Dashboard")
    st.write("Historical analytics from daily aggregated IoT sensor data.")

    df = get_daily_data()

    if df.empty:
        st.warning("No aggregated data found. Run aggregate_daily.py first.")
        return

    # Convert date field to datetime
    df["day"] = pd.to_datetime(df["day"])

    # ------------------------------
    # Sidebar Filters
    # ------------------------------
    st.sidebar.header("Filters")

    buildings = ["(All)"] + sorted(df["building_id"].unique())
    floors = ["(All)"] + sorted(df["floor"].unique())
    rooms = ["(All)"] + sorted(df["room"].unique())
    dates = ["(All)"] + sorted(df["day"].dt.date.unique().astype(str))

    selected_building = st.sidebar.selectbox("Building", buildings)
    selected_floor = st.sidebar.selectbox("Floor", floors)
    selected_room = st.sidebar.selectbox("Room", rooms)
    selected_date = st.sidebar.selectbox("Date", dates)

    # Apply filters
    filtered = df.copy()

    if selected_building != "(All)":
        filtered = filtered[filtered["building_id"] == selected_building]
    if selected_floor != "(All)":
        filtered = filtered[filtered["floor"] == selected_floor]
    if selected_room != "(All)":
        filtered = filtered[filtered["room"] == selected_room]
    if selected_date != "(All)":
        filtered = filtered[filtered["day"].dt.date.astype(str) == selected_date]

    if filtered.empty:
        st.warning("No data for the selected filters.")
        return

    # ------------------------------
    # Summary Metrics
    # ------------------------------
    col1, col2, col3 = st.columns(3)

    col1.metric("Avg Temperature (°C)", round(filtered["temperature_c_mean"].mean(), 2))
    col2.metric("Avg Occupancy", round(filtered["occupancy_count_mean"].mean(), 2))
    col3.metric("Avg Energy (W)", round(filtered["energy_w_mean"].mean(), 2))

    st.markdown("---")

    # ------------------------------
    # Charts
    # ------------------------------
    st.subheader("Temperature Over Time")
    temp_chart = filtered.groupby("day")["temperature_c_mean"].mean()
    st.line_chart(temp_chart)

    st.subheader("Energy Usage Over Time")
    energy_chart = filtered.groupby("day")["energy_w_mean"].mean()
    st.line_chart(energy_chart)

    st.subheader("Occupancy Over Time")
    occ_chart = filtered.groupby("day")["occupancy_count_mean"].mean()
    st.line_chart(occ_chart)

    st.markdown("---")

    # ------------------------------
    # Raw Data Table
    # ------------------------------
    st.subheader("Daily Summary Records")
    st.dataframe(filtered.reset_index(drop=True))


if __name__ == "__main__":
    main()
