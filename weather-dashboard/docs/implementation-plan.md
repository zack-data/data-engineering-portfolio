# Implementation Plan — Weather Dashboard (Airflow MVP)

> Builds the pipeline described in `docs/research.md` using Apache Airflow to ingest 7-day forecasts from Open-Meteo into DuckDB, with a Streamlit dashboard reading the same DuckDB file. Phases 0–5 build and run the pipeline directly on macOS via `airflow standalone`; Phase 6 polishes documentation so the repo is clone-and-run for someone else.

---

## Guiding Principles

1. **End-to-end first, polish later.** Get a single hardcoded city flowing all the way from Open-Meteo to a rendered Streamlit chart before adding cities, tests, or styling.
2. **Pipeline logic lives outside the DAG.** Functions in `pipeline/` are pure and unit-testable; the DAG just wires them together via `PythonOperator`.
3. **Schema lives in one place.** `pipeline/load.py` owns the `forecast_daily` DDL. Both the DAG and the dashboard read column names from it — no schema duplication in SQL strings scattered across files.
4. **Idempotent by design.** Every DAG run upserts the same `(city, forecast_date)` rows. Reruns are safe; no backfill logic needed because we're showing the *current* forecast, not historicals.

---

## Final Project Structure

```
weather-dashboard/
├── docs/
│   ├── research.md
│   └── implementation-plan.md          ← this file
├── dags/
│   └── weather_forecast_dag.py         ← schedule + task wiring only
├── pipeline/
│   ├── __init__.py
│   ├── config.py                       ← env vars, cities list, DuckDB path
│   ├── extract.py                      ← stage 1: Open-Meteo → raw JSON on disk
│   ├── transform.py                    ← stage 2: raw JSON → tidy rows
│   └── load.py                         ← stage 3: tidy rows → DuckDB upsert
├── dashboard/
│   └── app.py                          ← stage 4: DuckDB → Streamlit UI
├── config/
│   └── cities.yaml                     ← list of cities (Phase 5+)
├── data/
│   ├── raw/                            ← staged JSON, gitignored
│   └── weather.duckdb                  ← created at runtime, gitignored
├── tests/
│   ├── fixtures/
│   │   └── london_sample.json          ← saved Open-Meteo response
│   └── test_transform.py
├── airflow/                            ← AIRFLOW_HOME, gitignored
├── .env.example
├── .env                                ← gitignored
├── .gitignore
├── requirements.txt
├── Makefile
└── README.md
```

---

## Phase 0 — Project Scaffolding

**Goal:** Empty but runnable structure. No business logic yet.

### Files to create

**`requirements.txt`**

```
apache-airflow==2.10.4
duckdb==1.1.3
streamlit==1.39.0
pandas==2.2.3
requests==2.32.3
pyyaml==6.0.2
plotly==5.24.0
```

> Install with the constraints file to avoid Airflow's dependency-resolution headaches:
> `pip install -r requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"`

**`.gitignore`**

```
.env
data/
airflow/
__pycache__/
*.pyc
.venv/
```

**`.env.example`**

```
AIRFLOW_HOME=./airflow
DUCKDB_PATH=./data/weather.duckdb
RAW_DATA_DIR=./data/raw
DEFAULT_CITY=London
DEFAULT_LATITUDE=51.5074
DEFAULT_LONGITUDE=-0.1278
```

> No API key required — Open-Meteo is public.

**`Makefile`**

```makefile
.PHONY: airflow trigger dashboard test clean

airflow:
	AIRFLOW_HOME=$(PWD)/airflow airflow standalone

trigger:
	AIRFLOW_HOME=$(PWD)/airflow airflow dags trigger weather_forecast

dashboard:
	streamlit run dashboard/app.py

test:
	pytest tests/

clean:
	rm -rf data/raw/* data/weather.duckdb __pycache__ pipeline/__pycache__
```

### Verification

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"
mkdir -p data/raw airflow
cp .env.example .env
```

---

## Phase 1 — Shared Modules

### `pipeline/config.py`

Loads `.env` once, exposes typed constants.

```python
import os
from dotenv import load_dotenv

load_dotenv()

DUCKDB_PATH    = os.environ["DUCKDB_PATH"]
RAW_DATA_DIR   = os.environ["RAW_DATA_DIR"]

DEFAULT_CITY      = os.environ.get("DEFAULT_CITY", "London")
DEFAULT_LATITUDE  = float(os.environ.get("DEFAULT_LATITUDE", "51.5074"))
DEFAULT_LONGITUDE = float(os.environ.get("DEFAULT_LONGITUDE", "-0.1278"))

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
FORECAST_DAYS  = 7
```

### Verification

```bash
python -c "from pipeline.config import DUCKDB_PATH, OPEN_METEO_URL; print(DUCKDB_PATH, OPEN_METEO_URL)"
```

---

## Phase 2 — Extract (Open-Meteo Client)

**Goal:** Pull a 7-day forecast for one hardcoded city and write the raw JSON to disk.

### `pipeline/extract.py`

```python
import json
import pathlib
from datetime import datetime, timezone

import requests

from pipeline.config import (
    OPEN_METEO_URL, FORECAST_DAYS, RAW_DATA_DIR,
    DEFAULT_CITY, DEFAULT_LATITUDE, DEFAULT_LONGITUDE,
)

DAILY_FIELDS = "temperature_2m_max,temperature_2m_min,precipitation_sum,weathercode"

def fetch_forecast(latitude: float, longitude: float) -> dict:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": DAILY_FIELDS,
        "timezone": "auto",
        "forecast_days": FORECAST_DAYS,
    }
    resp = requests.get(OPEN_METEO_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()

def save_raw(payload: dict, city: str, run_ts: datetime) -> pathlib.Path:
    stamp = run_ts.strftime("%Y%m%dT%H%M%SZ")
    path = pathlib.Path(RAW_DATA_DIR) / city / f"{stamp}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))
    return path

if __name__ == "__main__":
    payload = fetch_forecast(DEFAULT_LATITUDE, DEFAULT_LONGITUDE)
    path = save_raw(payload, DEFAULT_CITY, datetime.now(timezone.utc))
    print(f"wrote {path} — {len(payload['daily']['time'])} days")
```

### Concepts learned here

- **Why save raw JSON to disk?** It gives you replayability if the transform changes. If the dashboard suddenly shows wrong values, you can re-run `transform` against yesterday's saved JSON without re-hitting the API.
- **Timezone handling.** `timezone=auto` makes Open-Meteo return forecast dates in the *city's* local time, not UTC. That's what the dashboard wants — "Tuesday's high in London" means London time.
- **`forecast_days=7`.** Open-Meteo defaults to 7 anyway, but setting it explicitly documents intent and prevents silent breakage if the default changes.

### Verification

```bash
python -m pipeline.extract
ls data/raw/London/
# pretty-print the daily section to confirm shape
python -c "import json; d=json.load(open('data/raw/London/$(ls data/raw/London | head -1)')); print(list(d['daily'].keys()))"
```

You should see 7 entries in `daily.time` and parallel arrays for each field in `DAILY_FIELDS`.

---

## Phase 3 — Transform

**Goal:** Convert raw Open-Meteo JSON into tidy rows ready for DuckDB.

### `pipeline/transform.py`

```python
from datetime import datetime, timezone

WMO_LABELS = {
    0: "clear",
    1: "mainly_clear", 2: "partly_cloudy", 3: "overcast",
    45: "fog", 48: "fog",
    51: "drizzle", 53: "drizzle", 55: "drizzle",
    61: "rain", 63: "rain", 65: "rain",
    71: "snow", 73: "snow", 75: "snow",
    80: "rain_showers", 81: "rain_showers", 82: "rain_showers",
    95: "thunderstorm", 96: "thunderstorm", 99: "thunderstorm",
}

def transform(payload: dict, city: str) -> list[dict]:
    daily = payload["daily"]
    ingested_at = datetime.now(timezone.utc)
    latitude  = payload["latitude"]
    longitude = payload["longitude"]

    rows = []
    for i, date_str in enumerate(daily["time"]):
        code = daily["weathercode"][i]
        rows.append({
            "city": city,
            "latitude": latitude,
            "longitude": longitude,
            "forecast_date": date_str,
            "temp_max_c": daily["temperature_2m_max"][i],
            "temp_min_c": daily["temperature_2m_min"][i],
            "precipitation_mm": daily["precipitation_sum"][i],
            "weather_code": code,
            "weather_label": WMO_LABELS.get(code, "unknown"),
            "ingested_at": ingested_at,
        })
    return rows
```

### `tests/test_transform.py`

```python
import json
from pathlib import Path
from pipeline.transform import transform

FIXTURE = Path(__file__).parent / "fixtures" / "london_sample.json"

def test_returns_seven_rows():
    payload = json.loads(FIXTURE.read_text())
    rows = transform(payload, city="London")
    assert len(rows) == 7

def test_required_columns_present():
    payload = json.loads(FIXTURE.read_text())
    rows = transform(payload, city="London")
    expected = {
        "city", "latitude", "longitude", "forecast_date",
        "temp_max_c", "temp_min_c", "precipitation_mm",
        "weather_code", "weather_label", "ingested_at",
    }
    assert expected <= set(rows[0].keys())

def test_weather_label_resolves_known_codes():
    payload = json.loads(FIXTURE.read_text())
    rows = transform(payload, city="London")
    for row in rows:
        assert row["weather_label"] != "unknown" or row["weather_code"] not in {0,1,2,3,61,63,65}
```

### Concepts learned here

- **Pure function = trivially testable.** `transform` takes a dict, returns a list of dicts. No I/O, no network, no DB. The test runs in milliseconds against a saved JSON fixture.
- **WMO weather codes.** Open-Meteo follows the WMO 4677 standard. We only need a coarse mapping for emoji selection — `clear`/`cloudy`/`rain`/`snow`/`thunderstorm` is enough for a dashboard.
- **Why string `forecast_date`, not a `date` object?** DuckDB casts ISO date strings to `DATE` automatically when the column type is `DATE`. Skipping the manual `date.fromisoformat()` keeps the function dependency-free.

### Verification

```bash
pytest tests/test_transform.py -v
```

---

## Phase 4 — Load (DuckDB Upsert)

**Goal:** Idempotent write into `forecast_daily`.

### `pipeline/load.py`

```python
import duckdb

from pipeline.config import DUCKDB_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS forecast_daily (
    city             VARCHAR,
    latitude         DOUBLE,
    longitude        DOUBLE,
    forecast_date    DATE,
    temp_max_c       DOUBLE,
    temp_min_c       DOUBLE,
    precipitation_mm DOUBLE,
    weather_code     INTEGER,
    weather_label    VARCHAR,
    ingested_at      TIMESTAMP,
    PRIMARY KEY (city, forecast_date)
);
"""

UPSERT = """
INSERT INTO forecast_daily
    (city, latitude, longitude, forecast_date,
     temp_max_c, temp_min_c, precipitation_mm,
     weather_code, weather_label, ingested_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (city, forecast_date) DO UPDATE SET
    temp_max_c       = excluded.temp_max_c,
    temp_min_c       = excluded.temp_min_c,
    precipitation_mm = excluded.precipitation_mm,
    weather_code     = excluded.weather_code,
    weather_label    = excluded.weather_label,
    ingested_at      = excluded.ingested_at;
"""

def get_conn(db_path: str = DUCKDB_PATH) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path)

def ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(SCHEMA)

def upsert(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> int:
    for r in rows:
        conn.execute(UPSERT, [
            r["city"], r["latitude"], r["longitude"], r["forecast_date"],
            r["temp_max_c"], r["temp_min_c"], r["precipitation_mm"],
            r["weather_code"], r["weather_label"], r["ingested_at"],
        ])
    return len(rows)
```

### Concepts learned here

- **`ON CONFLICT ... DO UPDATE`.** DuckDB 0.9+ supports Postgres-style upserts. Combined with the `(city, forecast_date)` PK, this is exactly the right primitive: each hourly run overwrites the same 7 rows per city with the latest forecast values.
- **Why no `MERGE`?** DuckDB doesn't have native `MERGE` in older versions, and `INSERT ... ON CONFLICT` does the same job with less ceremony.
- **Connection per task, not global.** The DAG opens a fresh connection in `load_to_duckdb` and closes it. DuckDB locks the file for writers, so leaving a connection open across tasks would block the dashboard's read-only connection from opening.

### Verification (end-to-end smoke, no Airflow yet)

```bash
python -c "
from datetime import datetime, timezone
from pipeline.extract import fetch_forecast, save_raw
from pipeline.transform import transform
from pipeline.load import get_conn, ensure_schema, upsert
from pipeline.config import DEFAULT_CITY, DEFAULT_LATITUDE, DEFAULT_LONGITUDE

payload = fetch_forecast(DEFAULT_LATITUDE, DEFAULT_LONGITUDE)
save_raw(payload, DEFAULT_CITY, datetime.now(timezone.utc))
rows = transform(payload, DEFAULT_CITY)
conn = get_conn()
ensure_schema(conn)
n = upsert(conn, rows)
print(f'upserted {n} rows')
print(conn.execute('SELECT count(*) FROM forecast_daily').fetchone())
"
duckdb data/weather.duckdb -c "SELECT forecast_date, temp_max_c, temp_min_c, weather_label FROM forecast_daily ORDER BY forecast_date;"
```

Run the above twice. Row count stays at 7; `ingested_at` advances. That's idempotency confirmed.

---

## Phase 5 — Airflow DAG

**Goal:** Same end-to-end flow, scheduled hourly via Airflow, with a post-load data quality check.

### `dags/weather_forecast_dag.py`

```python
from datetime import datetime, timedelta, timezone
import json
import pathlib

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator

from pipeline.config import (
    DEFAULT_CITY, DEFAULT_LATITUDE, DEFAULT_LONGITUDE, DUCKDB_PATH,
)
from pipeline.extract import fetch_forecast, save_raw
from pipeline.transform import transform
from pipeline.load import get_conn, ensure_schema, upsert

CITIES = [
    {"name": DEFAULT_CITY, "lat": DEFAULT_LATITUDE, "lon": DEFAULT_LONGITUDE},
]

def _extract(**ctx):
    run_ts = ctx["logical_date"]
    paths = []
    for city in CITIES:
        payload = fetch_forecast(city["lat"], city["lon"])
        path = save_raw(payload, city["name"], run_ts)
        paths.append({"city": city["name"], "path": str(path)})
    ctx["ti"].xcom_push(key="raw_paths", value=paths)

def _transform(**ctx):
    paths = ctx["ti"].xcom_pull(task_ids="extract_forecast", key="raw_paths")
    all_rows = []
    for entry in paths:
        payload = json.loads(pathlib.Path(entry["path"]).read_text())
        all_rows.extend(transform(payload, entry["city"]))
    # XCom is fine for ~tens of rows; switch to a temp file if cities ever grow large.
    ctx["ti"].xcom_push(key="rows", value=all_rows)

def _load(**ctx):
    rows = ctx["ti"].xcom_pull(task_ids="transform_forecast", key="rows")
    conn = get_conn()
    ensure_schema(conn)
    n = upsert(conn, rows)
    conn.close()
    print(f"upserted {n} rows for {len({r['city'] for r in rows})} cities")

with DAG(
    dag_id="weather_forecast",
    description="Hourly 7-day forecast ingestion from Open-Meteo",
    schedule="@hourly",
    start_date=datetime(2026, 5, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["weather", "open-meteo"],
) as dag:
    extract_forecast = PythonOperator(
        task_id="extract_forecast",
        python_callable=_extract,
    )
    transform_forecast = PythonOperator(
        task_id="transform_forecast",
        python_callable=_transform,
    )
    load_to_duckdb = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=_load,
    )
    check_row_count = SQLCheckOperator(
        task_id="check_row_count",
        conn_id="duckdb_default",   # configured via Airflow Connection: conn_type=duckdb, host=$DUCKDB_PATH
        sql=f"""
            SELECT count(*) >= {7 * len(CITIES)}
            FROM forecast_daily
            WHERE forecast_date >= current_date
        """,
    )

    extract_forecast >> transform_forecast >> load_to_duckdb >> check_row_count
```

### Concepts learned here

- **`schedule="@hourly"` + `catchup=False`.** Run on the next hour boundary; don't replay every missed hour since `start_date`. For a portfolio piece, catchup is noise.
- **`max_active_runs=1`.** Prevents two concurrent DAG runs from racing on the DuckDB writer lock if a run somehow overruns the hour.
- **XCom for handoff.** `extract` writes raw JSON to disk and pushes file paths; `transform` reads them and pushes rows; `load` consumes the rows. Small payloads (~tens of rows) fit in XCom comfortably; if we ever add hundreds of cities, switch to a temp file path on disk.
- **`SQLCheckOperator` data quality gate.** Asserts at least `7 * num_cities` rows exist with `forecast_date >= today`. Catches the obvious failure mode where extract or transform silently drops rows. Cheap insurance.
- **Pipeline logic stays in `pipeline/`.** The DAG file is thin: it imports pure functions and wires them. This means the entire pipeline is testable without spinning up Airflow.

### Verification

```bash
make airflow
# wait for "Airflow is ready" — note the admin password printed to the console
# open http://localhost:8080, find weather_forecast, unpause it, trigger a run

# or trigger from CLI:
make trigger
```

Confirm all four tasks go green. Inspect logs for `check_row_count` — it should print the SQL and a row count ≥ 7.

---

## Phase 6 — Streamlit Dashboard

**Goal:** Read-only view of the next 7 days for the configured city.

### `dashboard/app.py`

```python
from datetime import datetime, timezone

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

WEATHER_EMOJI = {
    "clear": "☀", "mainly_clear": "🌤", "partly_cloudy": "⛅", "overcast": "☁",
    "fog": "🌫", "drizzle": "🌦", "rain": "🌧", "rain_showers": "🌧",
    "snow": "❄", "thunderstorm": "⛈", "unknown": "·",
}

@st.cache_resource
def get_db():
    return duckdb.connect(DUCKDB_PATH, read_only=True)

@st.cache_data(ttl=300)
def load_forecast(city: str) -> pd.DataFrame:
    return get_db().execute("""
        SELECT forecast_date, temp_max_c, temp_min_c,
               precipitation_mm, weather_label, ingested_at
        FROM forecast_daily
        WHERE city = ?
        ORDER BY forecast_date
    """, [city]).fetchdf()

@st.cache_data(ttl=300)
def list_cities() -> list[str]:
    return [r[0] for r in get_db().execute(
        "SELECT DISTINCT city FROM forecast_daily ORDER BY city"
    ).fetchall()]

cities = list_cities()
if not cities:
    st.warning("No forecast data yet — trigger the Airflow DAG first.")
    st.stop()

city = st.sidebar.selectbox("City", cities)
df = load_forecast(city)

st.markdown(f"# 7-Day Forecast — {city}")

# ---------- day cards ----------
cols = st.columns(len(df))
for col, row in zip(cols, df.itertuples()):
    emoji = WEATHER_EMOJI.get(row.weather_label, "·")
    col.markdown(f"""
        **{row.forecast_date.strftime('%a %d %b')}**

        {emoji}

        {row.temp_max_c:.0f}° / {row.temp_min_c:.0f}°

        {row.precipitation_mm:.1f} mm
    """)

# ---------- temperature line chart ----------
fig = go.Figure()
fig.add_trace(go.Scatter(x=df["forecast_date"], y=df["temp_max_c"],
                         mode="lines+markers", name="High", line=dict(color="#FF3B30")))
fig.add_trace(go.Scatter(x=df["forecast_date"], y=df["temp_min_c"],
                         mode="lines+markers", name="Low",  line=dict(color="#0071E3")))
fig.update_layout(height=320, title="Temperature (°C)", margin=dict(l=0, r=0, t=40, b=0))
st.plotly_chart(fig, use_container_width=True)

# ---------- precipitation bar chart ----------
fig2 = go.Figure(go.Bar(x=df["forecast_date"], y=df["precipitation_mm"],
                        marker_color="#0071E3"))
fig2.update_layout(height=240, title="Precipitation (mm)", margin=dict(l=0, r=0, t=40, b=0))
st.plotly_chart(fig2, use_container_width=True)

# ---------- footer ----------
last_updated = df["ingested_at"].max()
st.caption(f"Last updated: {last_updated:%Y-%m-%d %H:%M UTC}")
```

### Concepts learned here

- **`read_only=True` on the dashboard's connection.** DuckDB locks the file for writers; opening read-only lets the dashboard share access while Airflow runs are writing.
- **`@st.cache_data(ttl=300)`.** 5-minute cache means rapid sidebar interactions don't re-query DuckDB, but a dashboard left open will still pick up new data on the next interaction after the TTL.
- **`@st.cache_resource` for the connection.** The connection itself is cached for the session — only the query results expire.
- **Reading `last_updated` from the data.** The dashboard's "freshness" indicator is `max(ingested_at)`, so the user always knows whether they're seeing live data or a stale DuckDB file.

### Verification

```bash
make dashboard
# opens http://localhost:8501
```

You should see 7 day-cards, a temperature line chart, a precipitation bar chart, and a "Last updated" footer.

---

## Phase 7 — Multi-City

**Goal:** Drive the pipeline from `config/cities.yaml` instead of a single hardcoded city.

### `config/cities.yaml`

```yaml
cities:
  - name: London
    latitude: 51.5074
    longitude: -0.1278
  - name: New York
    latitude: 40.7128
    longitude: -74.0060
  - name: Tokyo
    latitude: 35.6762
    longitude: 139.6503
  - name: Sydney
    latitude: -33.8688
    longitude: 151.2093
```

### Changes

- `pipeline/config.py` — add `load_cities()` that reads the YAML and returns a list of dicts.
- `dags/weather_forecast_dag.py` — replace the hardcoded `CITIES` constant with `CITIES = load_cities()`.
- `check_row_count` — assertion now uses `7 * len(CITIES)` automatically.
- Dashboard — sidebar already reads distinct cities from DuckDB, so it picks up the new ones with no code change.

### Verification

```bash
make trigger
# after the run completes:
duckdb data/weather.duckdb -c "SELECT city, count(*) FROM forecast_daily GROUP BY city ORDER BY city;"
```

Each city should have exactly 7 rows. Dashboard's city selector now lists all four.

---

## Phase 8 — Documentation & Demo Polish

**Goal:** Repo is something a stranger can clone and run.

### Tasks

1. Flesh out `README.md`:
   - One-paragraph "what this is"
   - Architecture diagram (copy from `research.md`)
   - Quickstart: venv → install (with constraints URL) → `make airflow` → trigger DAG → `make dashboard`
   - Link to `docs/research.md` and `docs/implementation-plan.md`
   - Call out the deliberate choices (DuckDB over Postgres, `airflow standalone` over Docker)
2. Add a screenshot of the dashboard to `docs/`.
3. Sanity-check the quickstart in a fresh venv on a clean clone.

---

## Validation Checklist

Before declaring done:

- [ ] `pip install -r requirements.txt` (with constraints) succeeds in a fresh venv
- [ ] `python -m pipeline.extract` writes a valid JSON file with 7 daily entries
- [ ] `pytest tests/` passes
- [ ] End-to-end smoke (extract → transform → load) leaves 7 rows in `forecast_daily` and is idempotent on rerun
- [ ] `make airflow` boots and the `weather_forecast` DAG appears in the UI
- [ ] All four DAG tasks (`extract_forecast`, `transform_forecast`, `load_to_duckdb`, `check_row_count`) go green
- [ ] `check_row_count` correctly fails when `forecast_daily` is artificially emptied (`DELETE FROM forecast_daily`) — confirms the gate works
- [ ] `make dashboard` renders 7 day-cards, both charts, and a "Last updated" footer
- [ ] Multi-city: `cities.yaml` with 4 cities → dashboard shows 4 cities in the sidebar, each with 7 rows
- [ ] Killing the scheduler mid-run and restarting does not produce duplicate `(city, forecast_date)` rows
- [ ] README quickstart works on a fresh clone

---

## Known Limitations of This MVP (Future Work)

- **No forecast history.** We overwrite `(city, forecast_date)` on every run, so we can't answer "how did the forecast for May 10th evolve over the previous week?". Add a separate `forecast_history` table that appends rather than upserts.
- **Daily granularity only.** Open-Meteo also returns hourly data. Adding it is a new task + new table; out of scope for v1.
- **No alerting.** The `SQLCheckOperator` fails the task on data-quality breaches but doesn't notify anyone. Wire up `EmailOperator` on `on_failure_callback`, or push to a webhook.
- **`airflow standalone` is single-process.** Fine for local; production would split scheduler/webserver/workers and use Postgres for the metadata DB.
- **No CI.** A GitHub Actions workflow running `pytest` and a DAG-import smoke test (`python dags/weather_forecast_dag.py`) is a cheap addition.
- **Hardcoded units.** Open-Meteo defaults to °C and mm. Adding a `?temperature_unit=fahrenheit` param and a dashboard toggle is straightforward but deferred.
