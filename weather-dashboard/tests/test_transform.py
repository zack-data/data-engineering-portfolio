import json
from pathlib import Path

from pipeline.transform import transform

FIXTURE = Path(__file__).parent / "fixtures" / "london_sample.json"


def _load():
    return json.loads(FIXTURE.read_text())


def test_returns_seven_rows():
    rows = transform(_load(), city="London")
    assert len(rows) == 7


def test_required_columns_present():
    rows = transform(_load(), city="London")
    expected = {
        "city", "latitude", "longitude", "forecast_date",
        "temp_max_c", "temp_min_c", "precipitation_mm",
        "weather_code", "weather_label", "ingested_at",
    }
    assert expected <= set(rows[0].keys())


def test_city_is_attached():
    rows = transform(_load(), city="London")
    assert all(r["city"] == "London" for r in rows)


def test_weather_label_resolves_known_codes():
    rows = transform(_load(), city="London")
    labels = {r["weather_code"]: r["weather_label"] for r in rows}
    assert labels[0] == "clear"
    assert labels[1] == "mainly_clear"
    assert labels[2] == "partly_cloudy"
    assert labels[3] == "overcast"
    assert labels[61] == "rain"
    assert labels[63] == "rain"


def test_unknown_code_falls_back():
    payload = _load()
    payload["daily"]["weathercode"][0] = 999
    rows = transform(payload, city="London")
    assert rows[0]["weather_label"] == "unknown"


def test_forecast_dates_match_payload():
    payload = _load()
    rows = transform(payload, city="London")
    assert [r["forecast_date"] for r in rows] == payload["daily"]["time"]
