from datetime import datetime, timezone

WMO_LABELS = {
    0: "clear",
    1: "mainly_clear", 2: "partly_cloudy", 3: "overcast",
    45: "fog", 48: "fog",
    51: "drizzle", 53: "drizzle", 55: "drizzle",
    56: "drizzle", 57: "drizzle",
    61: "rain", 63: "rain", 65: "rain",
    66: "rain", 67: "rain",
    71: "snow", 73: "snow", 75: "snow", 77: "snow",
    80: "rain_showers", 81: "rain_showers", 82: "rain_showers",
    85: "snow", 86: "snow",
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
