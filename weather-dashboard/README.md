# Weather Dashboard

Apache Airflow pipeline that pulls 7-day forecasts from [Open-Meteo](https://open-meteo.com), persists them to DuckDB, and visualizes them in a Streamlit dashboard. Cities are configured in `config/cities.yaml`; the pipeline upserts on `(city, forecast_date)` so reruns are idempotent.

Open-Meteo is a public, no-auth API. No keys, no signup.

See [`docs/research.md`](docs/research.md) for design rationale and [`docs/implementation-plan.md`](docs/implementation-plan.md) for the build plan.

## Quick start

Requires Python 3.11 (Airflow 2.10 does not yet support 3.13). On macOS:
`brew install python@3.11`.

```bash
python3.11 -m venv .venv && source .venv/bin/activate
make install
cp .env.example .env
mkdir -p data/raw airflow

# Create Airflow Pipeline
make airflow              # terminal 1 — http://localhost:8080
                          # the admin password is printed to the console;
                          # also stored in airflow/simple_auth_manager_passwords.json.generated
make trigger              # terminal 2 — kicks off a run immediately
make dashboard            # terminal 3 — http://localhost:8501
```

The DAG runs `@hourly` once unpaused. `make trigger` forces a run so you don't have to wait for the next hour boundary.

## Components

| Stage     | Entry point                    | Purpose                                           |
| --------- | ------------------------------ | ------------------------------------------------- |
| Extract   | `pipeline/extract.py`          | Open-Meteo → raw JSON in `data/raw/{city}/`       |
| Transform | `pipeline/transform.py`        | Raw JSON → tidy rows, WMO code → label            |
| Load      | `pipeline/load.py`             | Upsert into `forecast_daily` (DuckDB)             |
| DAG       | `dags/weather_forecast_dag.py` | Wires the three above + a row-count quality check |
| Dashboard | `dashboard/app.py`             | Streamlit UI (read-only) over DuckDB              |

The DAG is a thin wrapper — every stage is a pure function in `pipeline/` and is unit-testable without spinning up Airflow.

```
extract_forecast → transform_forecast → load_to_duckdb → check_row_count
```

`check_row_count` asserts at least `7 × num_cities` rows exist for `forecast_date >= today`. If extract or transform silently drops rows, the DAG fails loudly.

## Configuration

| Variable        | Default                 | Notes                                          |
| --------------- | ----------------------- | ---------------------------------------------- |
| `DUCKDB_PATH`   | `./data/weather.duckdb` | Created on first write                         |
| `RAW_DATA_DIR`  | `./data/raw`            | Staged Open-Meteo JSON, partitioned by city    |
| `CITIES_CONFIG` | `./config/cities.yaml`  | Source of truth for which cities to forecast   |
| `AIRFLOW_HOME`  | `./airflow`             | Keeps Airflow's metadata DB out of `~/airflow` |

Add or remove cities by editing `config/cities.yaml` — both the DAG and the dashboard pick up changes automatically.

```yaml
cities:
  - name: London
    latitude: 51.5074
    longitude: -0.1278
  - name: New York
    latitude: 40.7128
    longitude: -74.0060
```

## Inspecting the data

```bash
duckdb data/weather.duckdb -c "
  SELECT city, forecast_date, temp_max_c, temp_min_c, weather_label
  FROM forecast_daily
  WHERE forecast_date >= current_date
  ORDER BY city, forecast_date;
"
```

## Running tests

```bash
make test
```

Unit tests cover `transform()` against a saved Open-Meteo fixture (`tests/fixtures/london_sample.json`) — no network required.

## Resetting state

```bash
make clean                # wipes DuckDB and staged JSON
rm -rf airflow            # also wipes Airflow's metadata DB
```

## Project layout

```
weather-dashboard/
├── dags/                   Airflow DAG (wiring only)
├── pipeline/               Pure ETL functions (extract / transform / load)
├── dashboard/              Streamlit app
├── config/cities.yaml      Cities to forecast
├── tests/                  Unit tests + fixtures
├── data/                   DuckDB + staged raw JSON (gitignored)
├── airflow/                AIRFLOW_HOME (gitignored)
└── docs/                   research.md, implementation-plan.md
```
