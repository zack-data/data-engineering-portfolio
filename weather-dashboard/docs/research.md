# Research: Airflow + Streamlit Weather Dashboard

## Goal

Build an Apache Airflow pipeline that ingests weather forecast data and feeds a Streamlit dashboard showing the weather for the next 7 days (today + 6 days ahead) for one or more cities.

## Architecture Overview

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Open-Meteo  │ ──▶ │   Airflow    │ ──▶ │   DuckDB     │ ──▶ │  Streamlit   │
│  Forecast    │     │   DAG        │     │  (forecast   │     │  Dashboard   │
│  API         │     │  (hourly)    │     │   table)     │     │  (read-only) │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
```

The two services (Airflow and Streamlit) are decoupled via DuckDB on disk. Airflow owns writes, Streamlit only reads.

## Data Source: Open-Meteo

Recommended over OpenWeatherMap, NOAA, and Visual Crossing because:

- **Free, no API key, no signup** — ideal for a portfolio repo someone else can clone and run
- Returns up to 16 days of forecast in a single call
- Stable JSON schema with daily and hourly granularity
- Generous rate limits (10k calls/day, non-commercial)

Endpoint: `https://api.open-meteo.com/v1/forecast`

Example request for daily forecast (today + next 6 days):

```
GET https://api.open-meteo.com/v1/forecast
    ?latitude=51.5074&longitude=-0.1278
    &daily=temperature_2m_max,temperature_2m_min,precipitation_sum,weathercode
    &timezone=auto
    &forecast_days=7
```

Returns one row per day with min/max temp, precipitation totals, and a WMO weather code that maps to icons (sunny, cloudy, rain, etc.).

## Pipeline Design

### Schedule

`@hourly` is the right cadence here. The forecast updates several times per day on Open-Meteo's side, and hourly ingestion keeps the dashboard fresh without hammering the API. `@daily` would also be defensible, but a portfolio piece that "updates throughout the day" is more interesting to demo.

Avoid sub-hourly schedules — the underlying forecast doesn't change that fast and you'd just churn rows.

### DAG Structure

A linear, three-task DAG is sufficient. Resist the urge to over-engineer:

```
extract_forecast  ──▶  transform_forecast  ──▶  load_to_duckdb
```

- **extract_forecast** — `PythonOperator` that calls Open-Meteo for each configured city, writes raw JSON to a staging path (e.g. `data/raw/{city}/{run_ts}.json`). Keeping raw JSON on disk gives you replayability if the transform changes.
- **transform_forecast** — flattens the daily arrays into a tidy table (one row per city per forecast date), adds `ingested_at`, normalizes the WMO weather code to a label.
- **load_to_duckdb** — upserts into `forecast_daily` keyed on `(city, forecast_date)`. DuckDB doesn't have native `MERGE` in older versions, but `INSERT ... ON CONFLICT DO UPDATE` works in 0.9+.

### Target Schema

```sql
CREATE TABLE IF NOT EXISTS forecast_daily (
    city            VARCHAR,
    latitude        DOUBLE,
    longitude       DOUBLE,
    forecast_date   DATE,
    temp_max_c      DOUBLE,
    temp_min_c      DOUBLE,
    precipitation_mm DOUBLE,
    weather_code    INTEGER,
    weather_label   VARCHAR,
    ingested_at     TIMESTAMP,
    PRIMARY KEY (city, forecast_date)
);
```

The PK makes upserts trivial: every hour you overwrite the same 7 rows per city with the latest forecast values.

### Idempotency

Each DAG run rewrites the same `(city, forecast_date)` rows for the next 7 days. Reruns are safe and don't produce duplicates. No backfill logic needed — historical forecasts aren't the goal here; we're showing the _current_ forecast.

If you later want to track "how did the forecast for May 10th change over time?", add a separate `forecast_history` table that appends rather than upserts. Out of scope for v1.

## Streamlit Layer

Streamlit reads directly from the same DuckDB file. Two patterns work:

1. **Direct query on page load** — simplest, fine for a single-user demo. Use `@st.cache_data(ttl=300)` so repeated interactions don't re-query.
2. **Read into pandas once per session** — cleaner if the dashboard has multiple charts.

Recommended page layout:

- City selector (sidebar)
- 7-day forecast as a row of cards (date, icon from weather code, high/low, precip)
- Line chart of high/low temps across the week
- Bar chart of precipitation
- Footer showing `max(ingested_at)` so the user knows how fresh the data is

## Local Setup Recommendation

```bash
pip install apache-airflow
airflow standalone
```

This runs the scheduler, webserver, and a SQLite metadata DB in one process. Avoid Docker Compose for the Airflow side unless the rest of the portfolio is containerized — it adds setup friction for anyone reviewing the repo.

Pin Airflow to a specific version in `requirements.txt` (e.g. `apache-airflow==2.10.*`) and use a constraints file to avoid dependency hell:

```
pip install "apache-airflow==2.10.4" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"
```

## Suggested Repo Layout

```
weather-dashboard/
├── README.md
├── requirements.txt
├── dags/
│   └── weather_forecast_dag.py
├── pipeline/
│   ├── extract.py          # Open-Meteo client
│   ├── transform.py        # JSON → tidy rows
│   └── load.py             # DuckDB upsert
├── dashboard/
│   └── app.py              # Streamlit entry point
├── data/
│   ├── raw/                # staged JSON, gitignored
│   └── weather.duckdb      # gitignored
├── config/
│   └── cities.yaml         # list of cities to track
└── docs/
    └── research.md
```

Keeping pipeline logic in `pipeline/` (not inside the DAG file) means the transforms are unit-testable without spinning up Airflow.

## Decisions Made

- **Storage: DuckDB, not Postgres.** Zero setup cost and matches the rest of the portfolio. Postgres would be more "production-shaped" but adds a service to run. Call this out in the README as a deliberate choice.
- **Scope: start single-city, extend later.** Hardcode one city (e.g. London) for the first end-to-end pass, then extend to a `cities.yaml`-driven loop once the single-city path works. Don't build the abstraction first.
- **Granularity: daily only for v1.** Open-Meteo also returns hourly data, but the 7-day daily summary is what the dashboard renders. Adding hourly later is straightforward (new task + new table).
- **Data quality: include a simple post-load check.** Add a `SQLCheckOperator` task downstream of `load_to_duckdb` asserting `SELECT count(*) >= 7 * num_cities FROM forecast_daily WHERE forecast_date >= current_date`. Cheap, demo-able, and catches the obvious failure mode where extract or transform silently drops rows.
- **Setup: `airflow standalone`, not Docker Compose.** Single-process scheduler + webserver + SQLite metadata DB. Lower friction.

## Next Steps

1. Scaffold the repo layout above.
2. Implement `extract.py` against Open-Meteo for a single hardcoded city; verify JSON shape.
3. Write `transform.py` with unit tests on a saved sample JSON.
4. Build the DAG, run via `airflow standalone`, confirm DuckDB populates.
5. Build the Streamlit dashboard against the populated DuckDB.
6. Extend to multiple cities via `cities.yaml`.
