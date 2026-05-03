# Bluesky Sentiment Dashboard

Kafka pipeline that streams the Bluesky Jetstream firehose, filters posts mentioning a configurable keyword, scores them with VADER, persists to DuckDB, and visualizes them in a Streamlit dashboard. The whole stack — single-node Kafka broker, three Python pipeline processes, and the dashboard — runs inside one Docker container.

Jetstream is a public WebSocket — no auth, no client_id. Posts are filtered server-side to `app.bsky.feed.post` commits, then locally to English posts mentioning a brand keyword.

See [`docs/implementation-plan.md`](docs/implementation-plan.md) for full design and rationale, and [`docs/research.md`](docs/research.md) for background.

## Quick start (Docker)

Requires Docker Desktop.

```bash
docker compose up --build
# open http://localhost:8501
```

First boot takes ~20s while Kafka formats its KRaft storage. The dashboard refreshes every minute. Match rate depends on keyword popularity — common words like "apple" land in seconds, niche brand names may go minutes between matches.

Change the tracked keyword without rebuilding:

```bash
BRAND_KEYWORDS=openai docker compose up
```

Wipe all state (DuckDB rows, Jetstream cursor, Kafka log):

```bash
docker compose down -v
rm -rf data/*.duckdb data/jetstream_cursor.txt
```

## Quick start (local, no Docker)

For development on the pipeline code itself. Requires a local Kafka broker on `localhost:9092` (e.g. `brew install kafka && brew services start kafka`).

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
mkdir -p data
cp .env.example .env

make topics                # one-time
make producer              # terminal 1
make enricher              # terminal 2
make sink                  # terminal 3
make dashboard             # terminal 4 -> http://localhost:8501
```

## Components

| Stage     | Entry point                 | Purpose                                   |
| --------- | --------------------------- | ----------------------------------------- |
| Producer  | `src/bluesky_producer.py`   | Jetstream WebSocket → `bluesky-raw-posts` |
| Enricher  | `src/sentiment_enricher.py` | VADER scoring → `sentiment-enriched`      |
| Sink      | `src/duckdb_sink.py`        | Upsert into `data/sentiment.duckdb`       |
| Dashboard | `src/dashboard.py`          | Streamlit UI over DuckDB                  |

Inside the container these four processes plus the Kafka broker are launched by `docker/start.sh`; if any one exits, the script tears the rest down so Docker restarts the container as a unit.

## Configuration

| Variable                  | Default                                              | Notes                                                   |
| ------------------------- | ---------------------------------------------------- | ------------------------------------------------------- |
| `BRAND_KEYWORDS`          | `apple`                                              | Comma-separated, case-insensitive                       |
| `LANG_FILTER`             | `en`                                                 | VADER is English-only                                   |
| `JETSTREAM_URL`           | `wss://jetstream2.us-east.bsky.network/subscribe`    | Public Jetstream endpoint                               |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092`                                     | Loopback inside the container; host-side broker locally |
| `DUCKDB_PATH`             | `/app/data/sentiment.duckdb` (Docker) / `./data/...` | Bind-mounted to `./data` in Compose                     |
| `JETSTREAM_CURSOR_PATH`   | `/app/data/jetstream_cursor.txt`                     | Lets the producer resume after a restart                |

To smoke-test pipeline wiring quickly, set `BRAND_KEYWORDS=the` for a few seconds and then revert.

## Data persistence

| Path                   | Survives `docker compose down` | Survives `down -v` |
| ---------------------- | :----------------------------: | :----------------: |
| `./data/` (bind mount) |              yes               |        yes         |
| `kafka-data` (volume)  |              yes               |         no         |

Inspect the DuckDB file from the host at any time:

```bash
duckdb data/sentiment.duckdb -c "SELECT COUNT(*), MIN(created_at), MAX(created_at) FROM sentiment_events;"
```
