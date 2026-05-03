import json
import pathlib
from datetime import datetime, timezone

import requests

from pipeline.config import (
    OPEN_METEO_URL, FORECAST_DAYS, RAW_DATA_DIR, load_cities,
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
    safe_city = city.replace(" ", "_")
    path = pathlib.Path(RAW_DATA_DIR) / safe_city / f"{stamp}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))
    return path


if __name__ == "__main__":
    run_ts = datetime.now(timezone.utc)
    for city in load_cities():
        payload = fetch_forecast(city["latitude"], city["longitude"])
        path = save_raw(payload, city["name"], run_ts)
        print(f"wrote {path} — {len(payload['daily']['time'])} days")
