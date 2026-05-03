from datetime import datetime

import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from pipeline.config import DUCKDB_PATH

st.set_page_config(
    page_title="7-Day Weather",
    page_icon="☀",
    layout="wide",
)

st.markdown("""
<style>
  .stApp { background-color: #F5F5F7; }
  body, .stMarkdown {
    font-family: -apple-system, BlinkMacSystemFont, "San Francisco",
                 "Helvetica Neue", sans-serif;
  }
  div[data-testid="stPlotlyChart"],
  div.day-card {
    background-color: #FFFFFF;
    border: 1px solid #E5E5E9;
    border-radius: 12px;
    padding: 16px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
  }
  div.day-card { text-align: center; }
  div.day-card .date    { font-weight: 600; color: #1D1D1F; font-size: 14px; }
  div.day-card .emoji   { font-size: 36px; margin: 8px 0; }
  div.day-card .temp    { font-size: 18px; color: #1D1D1F; }
  div.day-card .precip  { font-size: 12px; color: #6E6E73; margin-top: 4px; }
  h1 { font-weight: 700; color: #1D1D1F; }
</style>
""", unsafe_allow_html=True)

WEATHER_EMOJI = {
    "clear": "☀️", "mainly_clear": "🌤️", "partly_cloudy": "⛅",
    "overcast": "☁️", "fog": "🌫️", "drizzle": "🌦️", "rain": "🌧️",
    "rain_showers": "🌦️", "snow": "❄️", "thunderstorm": "⛈️",
    "unknown": "·",
}

APPLE_LAYOUT = dict(
    paper_bgcolor="#FFFFFF", plot_bgcolor="#FFFFFF",
    font=dict(family="-apple-system, BlinkMacSystemFont, sans-serif",
              color="#1D1D1F", size=13),
    xaxis=dict(gridcolor="#E5E5E9", linecolor="#E5E5E9"),
    yaxis=dict(gridcolor="#E5E5E9", linecolor="#E5E5E9"),
    margin=dict(l=0, r=0, t=40, b=0),
)


@st.cache_resource
def get_db() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DUCKDB_PATH, read_only=True)


@st.cache_data(ttl=300)
def list_cities() -> list[str]:
    return [r[0] for r in get_db().execute(
        "SELECT DISTINCT city FROM forecast_daily ORDER BY city"
    ).fetchall()]


@st.cache_data(ttl=300)
def load_forecast(city: str) -> pd.DataFrame:
    return get_db().execute("""
        SELECT forecast_date, temp_max_c, temp_min_c,
               precipitation_mm, weather_label, ingested_at
        FROM forecast_daily
        WHERE city = ? AND forecast_date >= current_date
        ORDER BY forecast_date
        LIMIT 7
    """, [city]).fetchdf()


# ---------- guard: empty DB ----------
try:
    cities = list_cities()
except duckdb.CatalogException:
    cities = []

if not cities:
    st.markdown("# 7-Day Weather")
    st.warning(
        "No forecast data yet. Start Airflow (`make airflow`), unpause the "
        "`weather_forecast` DAG, and trigger a run — or run `make smoke` for a "
        "one-shot ingest without Airflow."
    )
    st.stop()

# ---------- header ----------
city = st.sidebar.selectbox("City", cities)
df = load_forecast(city)

st.markdown(f"# 7-Day Forecast — {city}")

# ---------- day cards ----------
cols = st.columns(len(df))
for col, row in zip(cols, df.itertuples()):
    emoji = WEATHER_EMOJI.get(row.weather_label, "·")
    label = row.weather_label.replace("_", " ").title()
    col.markdown(f"""
        <div class="day-card">
            <div class="date">{row.forecast_date.strftime('%a %d %b')}</div>
            <div class="emoji">{emoji}</div>
            <div class="temp">{row.temp_max_c:.0f}° / {row.temp_min_c:.0f}°</div>
            <div class="precip">{label}<br>{row.precipitation_mm:.1f} mm</div>
        </div>
    """, unsafe_allow_html=True)

st.markdown("&nbsp;")

# ---------- temperature chart ----------
fig = go.Figure()
fig.add_trace(go.Scatter(
    x=df["forecast_date"], y=df["temp_max_c"],
    mode="lines+markers", name="High",
    line=dict(color="#FF3B30", width=2), marker=dict(size=8),
))
fig.add_trace(go.Scatter(
    x=df["forecast_date"], y=df["temp_min_c"],
    mode="lines+markers", name="Low",
    line=dict(color="#0071E3", width=2), marker=dict(size=8),
))
fig.update_layout(**APPLE_LAYOUT, height=320, title="Temperature (°C)")
st.plotly_chart(fig, use_container_width=True)

# ---------- precipitation chart ----------
fig2 = go.Figure(go.Bar(
    x=df["forecast_date"], y=df["precipitation_mm"],
    marker_color="#0071E3",
))
fig2.update_layout(**APPLE_LAYOUT, height=240, title="Precipitation (mm)")
st.plotly_chart(fig2, use_container_width=True)

# ---------- footer ----------
last_updated = pd.to_datetime(df["ingested_at"]).max()
st.caption(f"Last updated: {last_updated:%Y-%m-%d %H:%M UTC}")
