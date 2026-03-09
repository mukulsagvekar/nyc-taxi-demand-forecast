import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
from pathlib import Path
import json

st.set_page_config(layout="wide")

st.title("NYC Taxi Demand Forecast Dashboard")

# -----------------------------
# Snowflake Connection
# -----------------------------

@st.cache_data
def load_data():

    conn = snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )

    query = """
    SELECT
        f.ZONE_ID,
        z.Zone AS ZONE_NAME,
        z.BOROUGH,
        TIMESTAMP_FROM_PARTS(f.year, f.month, f.dayofmonth, f.hour, 0, 0) as DATETIME,
        ceil(f.trip_count) AS PREDICTED_DEMAND
    FROM NYCTAXI.FEATURE_STORE.ZONE_HOURLY_FORECAST f
    JOIN NYCTAXI.RAW.TAXI_ZONE_LOOKUP z
        ON f.ZONE_ID = z.locationid
    """

    df = pd.read_sql(query, conn)

    return df


df = load_data()

# -----------------------------
# Sidebar Filters
# -----------------------------

st.sidebar.header("Filters")

zone_list = sorted(df["ZONE_NAME"].unique())

selected_zones = st.sidebar.multiselect(
    "Select Zone",
    zone_list
)

BOROUGH_list = sorted(df["BOROUGH"].unique())

selected_BOROUGHs = st.sidebar.multiselect(
    "Select BOROUGH",
    BOROUGH_list
)

date_range = st.sidebar.date_input(
    "Select Date Range",
    [df.DATETIME.min(), df.DATETIME.max()]
)

# -----------------------------
# Apply Filters
# -----------------------------

filtered_df = df.copy()

if selected_zones:
    filtered_df = filtered_df[
        filtered_df.ZONE_NAME.isin(selected_zones)
    ]

if selected_BOROUGHs:
    filtered_df = filtered_df[
        filtered_df.BOROUGH.isin(selected_BOROUGHs)
    ]

filtered_df = filtered_df[
    (filtered_df.DATETIME >= pd.to_datetime(date_range[0])) &
    (filtered_df.DATETIME <= pd.to_datetime(date_range[1]))
]

# -----------------------------
# Forecast Line Chart
# -----------------------------

st.subheader("Taxi Demand Forecast")

fig = px.line(
    filtered_df,
    x="DATETIME",
    y="PREDICTED_DEMAND",
    color="ZONE_NAME",
)

st.plotly_chart(fig, use_container_width=True)

# -----------------------------
# Demand Aggregation
# -----------------------------

zone_demand = (
    filtered_df
    .groupby(["ZONE_ID","ZONE_NAME"])
    ["PREDICTED_DEMAND"]
    .mean()
    .reset_index()
)

# -----------------------------
# Load GeoJSON
# -----------------------------
# link to download geojson file - https://earthworks.stanford.edu/catalog/nyu-2451-36743

BASE_DIR = Path(__file__).resolve().parent
geo_path = BASE_DIR / "nyc_taxi_zones.geojson"

with open(geo_path) as f:
    geojson = json.load(f)

# -----------------------------
# Heatmap
# -----------------------------

st.subheader("NYC Taxi Demand Heatmap")

fig_map = px.choropleth_mapbox(
    zone_demand,
    geojson=geojson,
    locations="ZONE_ID",
    featureidkey="properties.locationid",
    color="PREDICTED_DEMAND",
    color_continuous_scale="Reds",
    hover_name="ZONE_NAME",
    mapbox_style="carto-positron",
    center={"lat":40.7128,"lon":-74.0060},
    zoom=10,
    opacity=0.7,
)

st.plotly_chart(fig_map, use_container_width=True)

# -----------------------------
# Top Demand Zones
# -----------------------------

st.subheader("Top Forecasted Demand Zones")

top_zones = zone_demand.sort_values(
    "PREDICTED_DEMAND",
    ascending=False
).head(10)

st.dataframe(top_zones)

# -----------------------------
# BOROUGH Demand
# -----------------------------

st.subheader("Demand by BOROUGH")

BOROUGH_demand = (
    filtered_df
    .groupby("BOROUGH")["PREDICTED_DEMAND"]
    .sum()
    .reset_index()
)

fig_bar = px.bar(
    BOROUGH_demand,
    x="BOROUGH",
    y="PREDICTED_DEMAND",
    color="BOROUGH"
)

st.plotly_chart(fig_bar, use_container_width=True)