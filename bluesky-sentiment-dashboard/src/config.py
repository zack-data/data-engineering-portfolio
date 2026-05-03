import os
from dotenv import load_dotenv

load_dotenv()

JETSTREAM_URL = os.environ["JETSTREAM_URL"]
KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
BRAND_KEYWORDS = [k.strip().lower() for k in os.environ["BRAND_KEYWORDS"].split(",")]
LANG_FILTER = os.environ.get("LANG_FILTER", "en")

DUCKDB_PATH = os.environ["DUCKDB_PATH"]
JETSTREAM_CURSOR_PATH = os.environ["JETSTREAM_CURSOR_PATH"]

TOPIC_RAW = "bluesky-raw-posts"
TOPIC_ENRICHED = "sentiment-enriched"
