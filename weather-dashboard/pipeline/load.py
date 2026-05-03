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
    import pathlib
    pathlib.Path(db_path).parent.mkdir(parents=True, exist_ok=True)
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
