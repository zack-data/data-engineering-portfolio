"""Hourly 7-day forecast ingestion from Open-Meteo into DuckDB.

extract_forecast -> transform_forecast -> load_to_duckdb -> check_row_count

All pipeline logic lives in `pipeline/`; this DAG only wires it together.
"""
from __future__ import annotations

import json
import pathlib
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.config import DUCKDB_PATH, load_cities
from pipeline.extract import fetch_forecast, save_raw
from pipeline.transform import transform
from pipeline.load import ensure_schema, get_conn, upsert

CITIES = load_cities()


def _extract(**ctx) -> None:
    run_ts = ctx["logical_date"]
    paths = []
    for city in CITIES:
        payload = fetch_forecast(city["latitude"], city["longitude"])
        path = save_raw(payload, city["name"], run_ts)
        paths.append({"city": city["name"], "path": str(path)})
    ctx["ti"].xcom_push(key="raw_paths", value=paths)


def _transform(**ctx) -> None:
    paths = ctx["ti"].xcom_pull(task_ids="extract_forecast", key="raw_paths")
    all_rows = []
    for entry in paths:
        payload = json.loads(pathlib.Path(entry["path"]).read_text())
        rows = transform(payload, entry["city"])
        for r in rows:
            r["ingested_at"] = r["ingested_at"].isoformat()
        all_rows.extend(rows)
    ctx["ti"].xcom_push(key="rows", value=all_rows)


def _load(**ctx) -> None:
    rows = ctx["ti"].xcom_pull(task_ids="transform_forecast", key="rows")
    for r in rows:
        r["ingested_at"] = datetime.fromisoformat(r["ingested_at"])
    conn = get_conn()
    ensure_schema(conn)
    n = upsert(conn, rows)
    conn.close()
    print(f"upserted {n} rows for {len({r['city'] for r in rows})} cities")


def _check_row_count(**_) -> None:
    expected_min = 7 * len(CITIES)
    conn = get_conn()
    (count,) = conn.execute("""
        SELECT count(*) FROM forecast_daily
        WHERE forecast_date >= current_date
    """).fetchone()
    conn.close()
    print(f"row count for forecast_date >= today: {count} (expected >= {expected_min})")
    if count < expected_min:
        raise ValueError(
            f"data quality check failed: only {count} rows for forecast_date >= today, "
            f"expected at least {expected_min}"
        )


with DAG(
    dag_id="weather_forecast",
    description="Hourly 7-day forecast ingestion from Open-Meteo into DuckDB",
    schedule="@hourly",
    start_date=datetime(2026, 5, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["weather", "open-meteo", "duckdb"],
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
    check_row_count = PythonOperator(
        task_id="check_row_count",
        python_callable=_check_row_count,
    )

    extract_forecast >> transform_forecast >> load_to_duckdb >> check_row_count
