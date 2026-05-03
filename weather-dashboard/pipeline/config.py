import os
import pathlib
from typing import TypedDict

import yaml
from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

DUCKDB_PATH   = os.environ.get("DUCKDB_PATH",   str(REPO_ROOT / "data" / "weather.duckdb"))
RAW_DATA_DIR  = os.environ.get("RAW_DATA_DIR",  str(REPO_ROOT / "data" / "raw"))
CITIES_CONFIG = os.environ.get("CITIES_CONFIG", str(REPO_ROOT / "config" / "cities.yaml"))

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
FORECAST_DAYS  = 7


class City(TypedDict):
    name: str
    latitude: float
    longitude: float


def load_cities(path: str = CITIES_CONFIG) -> list[City]:
    raw = yaml.safe_load(pathlib.Path(path).read_text())
    return [
        {"name": c["name"], "latitude": float(c["latitude"]), "longitude": float(c["longitude"])}
        for c in raw["cities"]
    ]
