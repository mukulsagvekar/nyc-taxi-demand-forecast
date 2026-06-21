"""
app.py — MobilityIQ: NYC Taxi Demand Forecasting & Analytics Platform
---------------------------------------------------------------------------------------
Connects to Snowflake using credentials from st.secrets (works locally via
.streamlit/secrets.toml, and on Streamlit Community Cloud via the app's
Secrets settings).

Ask MobilityIQ (chat) uses the Cortex Analyst external REST API with a
Programmatic Access Token (PAT) for authentication.

Run locally:
    streamlit run app.py

Deploy:
    Push this folder to a GitHub repo, then deploy on share.streamlit.io.
    Add the contents of .streamlit/secrets.toml to the app's Secrets settings
    (do NOT commit secrets.toml to the repo).
"""

import json
from pathlib import Path

import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import snowflake.connector

# ── Page config ────────────────────────────────────────────────────────────
st.set_page_config(page_title="MobilityIQ · Taxi Demand Forecasting", page_icon="🚕", layout="wide")

st.markdown("""
<style>
  [data-testid="stMetricValue"] { font-size: 1.9rem; }
  .section-header {
    font-size: 1.05rem; font-weight: 600; color: #6366f1;
    border-bottom: 2px solid #6366f1; padding-bottom: 4px; margin-bottom: 14px;
  }
</style>
""", unsafe_allow_html=True)

BOROUGH_COLORS = {
    "Manhattan": "#6366f1", "Brooklyn": "#f59e0b", "Queens": "#3b82f6",
    "Bronx": "#8b5cf6", "Staten Island": "#10b981",
}

FORECAST_TABLE = "NYCTAXI.ANALYTICS.MART_DEMAND_FORECAST"
TRIP_FACTS_TABLE = "NYCTAXI.ANALYTICS.MART_TRIP_FACTS"
SEASONALITY_TABLE = "NYCTAXI.ANALYTICS.MART_DEMAND_SEASONALITY"
SEMANTIC_MODEL_FILE = st.secrets["cortex"]["semantic_model_file"]  # e.g. "@NYCTAXI.ANALYTICS.SEMANTIC_MODELS/mobilityiq_semantic_model.yaml"

# ── Snowflake connection ──────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
        role=st.secrets["snowflake"].get("role"),
    )

def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetch_pandas_all()
    finally:
        cur.close()

# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data
def load_summary():
    return run_query(f"""
        select
            sum(predicted_demand)                                  as total_predicted_demand,
            avg(predicted_demand)                                  as avg_predicted_demand,
            count(distinct zone_id)                                as zones_covered,
            max(predicted_demand)                                  as peak_zone_demand
        from {FORECAST_TABLE}
    """)

@st.cache_data
def load_revenue_summary():
    return run_query(f"""
        select
            sum(total_amount)                                      as total_revenue,
            avg(total_amount)                                      as avg_revenue_per_trip,
            avg(fare_amount)                                       as avg_fare,
            avg(trip_distance)                                     as avg_trip_distance
        from {TRIP_FACTS_TABLE}
    """)

@st.cache_data
def load_borough_forecast():
    return run_query(f"""
        select
            borough,
            sum(predicted_demand)                                  as total_predicted_demand,
            avg(predicted_demand)                                  as avg_predicted_demand
        from {FORECAST_TABLE}
        group by borough
        order by total_predicted_demand desc
    """)

@st.cache_data
def load_top_zones():
    return run_query(f"""
        select
            zone_name, borough,
            avg(predicted_demand)                                  as avg_predicted_demand
        from {FORECAST_TABLE}
        group by zone_name, borough
        order by avg_predicted_demand desc
        limit 10
    """)

@st.cache_data
def load_hourly_pattern():
    return run_query(f"""
        select
            forecast_hour,
            avg(predicted_demand)                                  as avg_predicted_demand
        from {FORECAST_TABLE}
        group by forecast_hour
        order by forecast_hour
    """)

@st.cache_data
def load_revenue_by_borough():
    return run_query(f"""
        select
            borough,
            sum(total_amount)                                      as total_revenue,
            avg(total_amount)                                      as avg_revenue_per_trip
        from {TRIP_FACTS_TABLE}
        group by borough
        order by total_revenue desc
    """)

@st.cache_data
def load_holiday_lift():
    return run_query(f"""
        select
            case when is_holiday = 1 then 'Holiday' else 'Regular day' end as day_type,
            avg(trip_count)                                        as avg_trip_count
        from {SEASONALITY_TABLE}
        group by is_holiday
    """)

@st.cache_data
def load_map_data():
    return run_query(f"""
        select
            zone_id, zone_name, borough,
            avg(predicted_demand) as avg_predicted_demand
        from {FORECAST_TABLE}
        group by zone_id, zone_name, borough
    """)

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🚕 MobilityIQ — Taxi Demand Forecasting & Analytics")
st.caption("Taxi demand, revenue & seasonality · Semantic layer powered by Snowflake Cortex Analyst")
st.divider()

# ── KPI row ───────────────────────────────────────────────────────────────────
summary = load_summary().iloc[0]
rev_summary = load_revenue_summary().iloc[0]

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total predicted demand", f"{int(summary['TOTAL_PREDICTED_DEMAND']):,}")
c2.metric("Zones covered", f"{int(summary['ZONES_COVERED']):,}")
c3.metric("Avg revenue / trip", f"${rev_summary['AVG_REVENUE_PER_TRIP']:.2f}")
c4.metric("Avg fare", f"${rev_summary['AVG_FARE']:.2f}")
c5.metric("Avg trip distance", f"{rev_summary['AVG_TRIP_DISTANCE']:.1f} mi")

st.divider()

# ── Top zones + Borough forecast ────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.markdown('<div class="section-header">Top forecasted demand zones</div>', unsafe_allow_html=True)
    top_zones = load_top_zones()
    fig_top = px.bar(
        top_zones, x="AVG_PREDICTED_DEMAND", y="ZONE_NAME", orientation="h",
        color="BOROUGH", color_discrete_map=BOROUGH_COLORS,
        labels={"AVG_PREDICTED_DEMAND": "Avg predicted demand", "ZONE_NAME": ""},
    )
    fig_top.update_layout(height=340, margin=dict(l=0, r=10, t=10, b=30), yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_top, use_container_width=True)

with col2:
    st.markdown('<div class="section-header">Forecasted demand by borough</div>', unsafe_allow_html=True)
    borough_forecast = load_borough_forecast()
    fig_boro = px.bar(
        borough_forecast, x="BOROUGH", y="TOTAL_PREDICTED_DEMAND",
        color="BOROUGH", color_discrete_map=BOROUGH_COLORS,
        labels={"TOTAL_PREDICTED_DEMAND": "Total predicted demand", "BOROUGH": ""},
    )
    fig_boro.update_layout(showlegend=False, height=340, margin=dict(l=0, r=10, t=10, b=30))
    st.plotly_chart(fig_boro, use_container_width=True)

st.divider()

# ── Hourly pattern + Revenue by borough ─────────────────────────────────────
col3, col4 = st.columns(2)

with col3:
    st.markdown('<div class="section-header">Demand pattern by hour of day</div>', unsafe_allow_html=True)
    hourly = load_hourly_pattern()
    fig_hourly = px.line(
        hourly, x="FORECAST_HOUR", y="AVG_PREDICTED_DEMAND",
        labels={"FORECAST_HOUR": "Hour of day", "AVG_PREDICTED_DEMAND": "Avg predicted demand"},
        markers=True,
    )
    fig_hourly.update_layout(height=340, margin=dict(l=0, r=10, t=10, b=30))
    st.plotly_chart(fig_hourly, use_container_width=True)

with col4:
    st.markdown('<div class="section-header">Revenue by borough</div>', unsafe_allow_html=True)
    revenue = load_revenue_by_borough()
    fig_rev = px.bar(
        revenue, x="BOROUGH", y="TOTAL_REVENUE",
        color="BOROUGH", color_discrete_map=BOROUGH_COLORS,
        labels={"TOTAL_REVENUE": "Total revenue ($)", "BOROUGH": ""},
    )
    fig_rev.update_layout(showlegend=False, height=340, margin=dict(l=0, r=10, t=10, b=30))
    st.plotly_chart(fig_rev, use_container_width=True)

st.divider()

# ── Map ───────────────────────────────────────────────────────────────────────
st.markdown('<div class="section-header">Taxi demand heatmap</div>', unsafe_allow_html=True)

BASE_DIR = Path(__file__).resolve().parent
geo_path = BASE_DIR / "nyc_taxi_zones.geojson"
with open(geo_path) as f:
    geojson = json.load(f)

map_df = load_map_data()
fig_map = px.choropleth_mapbox(
    map_df, geojson=geojson,
    locations="ZONE_ID", featureidkey="properties.locationid",
    color="AVG_PREDICTED_DEMAND", color_continuous_scale="Reds",
    hover_name="ZONE_NAME",
    mapbox_style="carto-positron",
    center={"lat": 40.7128, "lon": -74.0060}, zoom=9.5,
    opacity=0.7, height=460,
)
fig_map.update_layout(margin=dict(l=0, r=0, t=0, b=0))
st.plotly_chart(fig_map, use_container_width=True)

st.divider()

# ── Holiday lift ──────────────────────────────────────────────────────────────
st.markdown('<div class="section-header">Holiday vs regular day demand</div>', unsafe_allow_html=True)
holiday = load_holiday_lift()
fig_holiday = px.bar(
    holiday, x="DAY_TYPE", y="AVG_TRIP_COUNT",
    color="DAY_TYPE", color_discrete_sequence=["#6366f1", "#94a3b8"],
    labels={"AVG_TRIP_COUNT": "Avg trip count", "DAY_TYPE": ""},
)
fig_holiday.update_layout(showlegend=False, height=300, margin=dict(l=0, r=10, t=10, b=30))
st.plotly_chart(fig_holiday, use_container_width=True)

st.divider()

# ── Ask MobilityIQ — Cortex Analyst chat (external REST API) ────────────────
st.markdown('<div class="section-header">💬 Ask MobilityIQ</div>', unsafe_allow_html=True)
st.caption(
    "Ask a question in plain English. Powered by Cortex Analyst + the semantic model. "
    "Covers forecasted demand, historical trip revenue/fares, and holiday/weekend "
    "seasonality. Does not cover forecast accuracy, anomaly detection, driver-level "
    "data, or real-time demand."
)

if "messages" not in st.session_state:
    st.session_state.messages = []

def call_cortex_analyst(prompt: str) -> dict:
    """Send a question to Cortex Analyst's REST API and return the parsed response."""
    account = st.secrets["snowflake"]["account"]
    # account identifier like "abc12345.us-east-1" -> hostname form
    host = account.replace("_", "-").lower()
    url = f"https://{host}.snowflakecomputing.com/api/v2/cortex/analyst/message"

    headers = {
        "Authorization": f"Bearer {st.secrets['cortex']['pat_token']}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    body = {
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": prompt}]}
        ],
        "semantic_model_file": SEMANTIC_MODEL_FILE,
    }

    resp = requests.post(url, headers=headers, json=body, timeout=60)
    if resp.status_code >= 400:
        raise RuntimeError(f"Cortex Analyst request failed ({resp.status_code}): {resp.text}")
    return resp.json()

def render_cortex_response(content_items):
    """Render the content blocks returned by Cortex Analyst (text, sql, suggestions)."""
    sql_to_run = None
    for item in content_items:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "sql":
            sql_to_run = item["statement"]
            with st.expander("View generated SQL"):
                st.code(sql_to_run, language="sql")
        elif item["type"] == "suggestions":
            st.write("You could also ask:")
            for s in item.get("suggestions", []):
                st.write(f"- {s}")

    if sql_to_run:
        try:
            # Cortex Analyst appends a trailing comment + semicolon to the
            # generated statement (e.g. "-- Generated by Cortex Analyst...;").
            # Strip both so execution is clean across drivers.
            clean_sql = sql_to_run.split("-- Generated by Cortex Analyst")[0].strip()
            clean_sql = clean_sql.rstrip(";").strip()

            result_df = run_query(clean_sql)
            st.dataframe(result_df, use_container_width=True)

            if result_df.shape[1] == 2 and result_df.shape[0] > 1:
                col_a, col_b = result_df.columns[0], result_df.columns[1]
                # Snowflake numeric aggregates (AVG, SUM on NUMBER columns)
                # often come back as Python Decimal -> pandas 'object' dtype,
                # which is_numeric_dtype() reports as False. Coerce explicitly
                # instead of relying on the inferred dtype.
                numeric_col_b = pd.to_numeric(result_df[col_b], errors="coerce")
                if numeric_col_b.notna().any():
                    chart_df = result_df.copy()
                    chart_df[col_b] = numeric_col_b
                    fig = px.bar(chart_df, x=col_a, y=col_b)
                    st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Couldn't run the generated SQL: {e}")

# Show chat history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        if msg["role"] == "user":
            st.markdown(msg["content"])
        else:
            render_cortex_response(msg["content"])

# Suggested starter questions
st.write("Try asking:")
examples = [
    "What's the average fare in Manhattan vs Queens?",
    "Is demand higher on holidays vs regular days?",
    "Which zone generates the most revenue per trip?",
]
example_cols = st.columns(len(examples))
for col, ex in zip(example_cols, examples):
    if col.button(ex, use_container_width=True):
        st.session_state.pending_prompt = ex

# Chat input
prompt = st.chat_input("Ask MobilityIQ a question about demand, revenue, or trends...")
if "pending_prompt" in st.session_state:
    prompt = st.session_state.pop("pending_prompt")

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                response = call_cortex_analyst(prompt)
                content_items = response["message"]["content"]
                render_cortex_response(content_items)
                st.session_state.messages.append({"role": "assistant", "content": content_items})
            except Exception as e:
                error_text = f"Sorry, I couldn't process that: {e}"
                st.error(error_text)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": [{"type": "text", "text": error_text}]
                })

st.divider()
st.caption("Data: TLC Trip Records · Semantic model: mobilityiq_semantic_model.yaml · Built with Streamlit + Snowflake Cortex Analyst")