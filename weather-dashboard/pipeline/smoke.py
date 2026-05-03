"""End-to-end smoke test: extract -> transform -> load, no Airflow.

Run with:  make smoke
"""
from datetime import datetime, timezone

from pipeline.config import load_cities
from pipeline.extract import fetch_forecast, save_raw
from pipeline.transform import transform
from pipeline.load import get_conn, ensure_schema, upsert


def main() -> None:
    run_ts = datetime.now(timezone.utc)
    conn = get_conn()
    ensure_schema(conn)

    total = 0
    for city in load_cities():
        payload = fetch_forecast(city["latitude"], city["longitude"])
        save_raw(payload, city["name"], run_ts)
        rows = transform(payload, city["name"])
        total += upsert(conn, rows)
        print(f"  {city['name']}: upserted {len(rows)} rows")

    count = conn.execute("SELECT count(*) FROM forecast_daily").fetchone()[0]
    print(f"upserted {total} rows; forecast_daily now has {count} rows")
    conn.close()


if __name__ == "__main__":
    main()
