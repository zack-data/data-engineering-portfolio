# Implementation Plan — Bluesky Sentiment Dashboard (VADER MVP)

> Builds the pipeline described in `docs/research.md` using VADER for sentiment analysis. Targets the brand keyword on the Bluesky Jetstream firehose. Phases 0–6 build and run the pipeline directly on macOS; Phase 7 packages the whole stack — Kafka broker, the three pipeline processes, and the dashboard — into a single Docker container so it can be run with Docker Desktop.

---

## Guiding Principles

1. **Walk before running.** Get each stage producing/consuming a single message end-to-end before adding logic. Verify in the Kafka CLI between steps.
2. **One process per concern.** Producer, enricher, sink, dashboard are four separate Python entrypoints. They share nothing in memory — only Kafka topics and the DuckDB file.
3. **Schema lives in one place.** A single `schemas.py` module defines the JSON message shapes. Every producer/consumer imports from it.
4. **Fail loudly.** No try/except swallowing in the MVP. If sentiment analysis blows up on a malformed post, we want to see it. The one exception is the Jetstream WebSocket reconnect loop — disconnects are normal and must be retried.

---

## Final Project Structure

```
bluesky-sentiment-dashboard/
├── docs/
│   ├── research.md
│   └── implementation-plan.md          ← this file
├── src/
│   ├── __init__.py
│   ├── config.py                       ← env vars, brand keywords, Jetstream URL
│   ├── schemas.py                      ← JSON message schemas (TypedDict)
│   ├── kafka_admin.py                  ← topic creation helper
│   ├── bluesky_producer.py             ← stage 1: Jetstream → bluesky-raw-posts
│   ├── sentiment_enricher.py           ← stage 2: raw → VADER → enriched
│   ├── duckdb_sink.py                  ← stage 3: enriched → DuckDB
│   └── dashboard.py                    ← stage 4: DuckDB → Streamlit UI
├── data/
│   ├── sentiment.duckdb                ← created at runtime, gitignored
│   └── jetstream_cursor.txt            ← last processed time_us, gitignored
├── .streamlit/
│   └── config.toml                     ← Apple-style theme
├── docker/
│   ├── kraft.properties                ← single-node Kafka KRaft config
│   └── start.sh                        ← container entrypoint (kafka + 4 procs)
├── Dockerfile                          ← Phase 7
├── docker-compose.yml                  ← Phase 7
├── .dockerignore                       ← Phase 7
├── .env.example
├── .env                                ← gitignored
├── .gitignore
├── requirements.txt
├── Makefile
└── README.md
```

---

## Phase 0 — Project Scaffolding (~15 min)

**Goal:** Empty but runnable structure. No business logic yet.

### Files to create

**`requirements.txt`**

```
websockets==13.1
atproto==0.0.55
confluent-kafka==2.6.1
vaderSentiment==3.3.2
duckdb==1.1.3
streamlit==1.39.0
plotly==5.24.0
python-dotenv==1.0.1
```

> `atproto` is optional — only needed if you later add `searchPosts` backfill or resolve DIDs to handles. Keep it in for forward compatibility.

**`.gitignore`**

```
.env
data/
__pycache__/
*.pyc
.venv/
.streamlit/secrets.toml
```

**`.env.example`**

```
JETSTREAM_URL=wss://jetstream2.us-east.bsky.network/subscribe
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
BRAND_KEYWORDS=fyxer
LANG_FILTER=en
DUCKDB_PATH=./data/sentiment.duckdb
JETSTREAM_CURSOR_PATH=./data/jetstream_cursor.txt
```

> No auth, no client_id, no client_secret. Jetstream is a public stream.

**`Makefile`**

```makefile
.PHONY: topics producer enricher sink dashboard clean

topics:
	python -m src.kafka_admin

producer:
	python -m src.bluesky_producer

enricher:
	python -m src.sentiment_enricher

sink:
	python -m src.duckdb_sink

dashboard:
	streamlit run src/dashboard.py

clean:
	rm -rf data/*.duckdb data/jetstream_cursor.txt __pycache__ src/__pycache__
```

**`.streamlit/config.toml`** (from research §7) — unchanged.

### Verification

```bash
pip install -r requirements.txt
mkdir -p data
cp .env.example .env  # tweak if needed; defaults work as-is
```

---

## Phase 1 — Shared Modules (~20 min)

### `src/config.py`

Loads `.env` once, exposes typed constants.

```python
import os
from dotenv import load_dotenv

load_dotenv()

JETSTREAM_URL  = os.environ["JETSTREAM_URL"]
KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
BRAND_KEYWORDS  = [k.strip().lower() for k in os.environ["BRAND_KEYWORDS"].split(",")]
LANG_FILTER     = os.environ.get("LANG_FILTER", "en")

DUCKDB_PATH           = os.environ["DUCKDB_PATH"]
JETSTREAM_CURSOR_PATH = os.environ["JETSTREAM_CURSOR_PATH"]

TOPIC_RAW       = "bluesky-raw-posts"
TOPIC_ENRICHED  = "sentiment-enriched"
```

### `src/schemas.py`

Single source of truth for message shapes.

```python
from typing import TypedDict, Literal, List

class RawPost(TypedDict):
    uri: str            # at://{did}/app.bsky.feed.post/{rkey}
    cid: str
    did: str            # author DID
    text: str
    created_at: str     # ISO 8601 string from record.createdAt
    langs: List[str]
    time_us: int        # Jetstream cursor — microseconds since epoch

class EnrichedPost(RawPost):
    sentiment_label: Literal["positive", "neutral", "negative"]
    sentiment_score: float    # VADER compound: -1.0 to 1.0
    sentiment_pos: float
    sentiment_neu: float
    sentiment_neg: float
```

### `src/kafka_admin.py`

Creates topics idempotently. **You already created `sentiment-enriched` from the previous Reddit version, so that one will be skipped — that's fine.** The new topic is `bluesky-raw-posts`. The old `reddit-raw-posts` topic is now unused; delete or ignore it.

```python
from confluent_kafka.admin import AdminClient, NewTopic
from src.config import KAFKA_BOOTSTRAP, TOPIC_RAW, TOPIC_ENRICHED

def main():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    topics = [
        NewTopic(TOPIC_RAW, num_partitions=3, replication_factor=1),
        NewTopic(TOPIC_ENRICHED, num_partitions=3, replication_factor=1),
    ]
    futures = admin.create_topics(topics)
    for name, future in futures.items():
        try:
            future.result()
            print(f"created topic: {name}")
        except Exception as e:
            print(f"topic {name}: {e}")

if __name__ == "__main__":
    main()
```

### Verification

```bash
make topics
kafka-topics --list --bootstrap-server localhost:9092
# should show: bluesky-raw-posts, sentiment-enriched (and the old reddit-raw-posts if you didn't delete it)
```

**Optional cleanup of stale topic:**

```bash
kafka-topics --delete --topic reddit-raw-posts --bootstrap-server localhost:9092
```

---

## Phase 2 — Bluesky Producer (~30 min)

**Goal:** Stream Jetstream commits, filter to English posts mentioning a brand keyword, write to `bluesky-raw-posts`.

### `src/bluesky_producer.py`

```python
import asyncio
import json
import os
import pathlib
import websockets
from confluent_kafka import Producer
from src.config import (
    JETSTREAM_URL, KAFKA_BOOTSTRAP, BRAND_KEYWORDS, LANG_FILTER,
    JETSTREAM_CURSOR_PATH, TOPIC_RAW,
)

POST_COLLECTION = "app.bsky.feed.post"

def matches_brand(text: str) -> bool:
    text = text.lower()
    return any(kw in text for kw in BRAND_KEYWORDS)

def delivery_report(err, msg):
    if err:
        print(f"delivery failed: {err}")
    else:
        print(f"  -> kafka {msg.topic()}[{msg.partition()}]@{msg.offset()}")

def load_cursor() -> int | None:
    p = pathlib.Path(JETSTREAM_CURSOR_PATH)
    if not p.exists():
        return None
    raw = p.read_text().strip()
    return int(raw) if raw else None

def save_cursor(time_us: int) -> None:
    pathlib.Path(JETSTREAM_CURSOR_PATH).write_text(str(time_us))

def build_url(cursor: int | None) -> str:
    base = f"{JETSTREAM_URL}?wantedCollections={POST_COLLECTION}"
    if cursor:
        base += f"&cursor={cursor}"
    return base

async def stream(producer: Producer):
    cursor = load_cursor()
    url = build_url(cursor)
    print(f"connecting: {url}")

    last_save = 0
    async with websockets.connect(url, max_size=2**20) as ws:
        async for raw in ws:
            evt = json.loads(raw)
            if evt.get("kind") != "commit":
                continue
            commit = evt.get("commit") or {}
            if commit.get("operation") != "create":
                continue
            if commit.get("collection") != POST_COLLECTION:
                continue

            record = commit.get("record") or {}
            text = record.get("text", "") or ""
            langs = record.get("langs") or []

            time_us = evt.get("time_us", 0)
            # Persist cursor every ~5s of stream time so a restart resumes cleanly.
            if time_us - last_save > 5_000_000:
                save_cursor(time_us)
                last_save = time_us

            if LANG_FILTER and LANG_FILTER not in langs:
                continue
            if not matches_brand(text):
                continue

            uri = f"at://{evt['did']}/{commit['collection']}/{commit['rkey']}"
            payload = {
                "uri": uri,
                "cid": commit.get("cid", ""),
                "did": evt["did"],
                "text": text,
                "created_at": record.get("createdAt", ""),
                "langs": langs,
                "time_us": time_us,
            }
            producer.produce(
                TOPIC_RAW,
                key=uri.encode(),
                value=json.dumps(payload).encode(),
                on_delivery=delivery_report,
            )
            producer.poll(0)
            print(f"[{evt['did'][:16]}…] {text[:80]}")

async def main():
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 10,
    })
    print(f"streaming Jetstream for keywords {BRAND_KEYWORDS} (lang={LANG_FILTER!r})")

    backoff = 1
    while True:
        try:
            await stream(producer)
        except (websockets.ConnectionClosed, OSError) as e:
            print(f"jetstream disconnected: {e!r} — reconnecting in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
        else:
            backoff = 1
        finally:
            producer.flush()

if __name__ == "__main__":
    asyncio.run(main())
```

### Bluesky / Kafka concepts learned here

- **Jetstream cursor**: `?cursor=<time_us>` resumes the stream from a specific microsecond timestamp. We persist it to a file so restarts don't double-process or lose recent events.
- **Server-side filtering**: `wantedCollections=app.bsky.feed.post` makes Jetstream skip likes, reposts, profile updates, etc. before sending bytes over the wire.
- **Async I/O**: The Bluesky firehose pushes events constantly. `websockets` is async-native, so we use `asyncio` here even though the rest of the pipeline is synchronous.
- **Producer config**: `linger.ms=10` batches messages for up to 10ms before sending — small latency cost, big throughput win.
- **Message key**: Setting `key=uri` ensures all events for the same post would land on the same partition (preserves ordering for that post).
- **Reconnect with backoff**: WebSocket drops are normal. Retry with exponential backoff capped at 30s.

### Verification

In one terminal:

```bash
make producer
```

In another, watch messages arrive:

```bash
kafka-console-consumer --topic bluesky-raw-posts \
  --bootstrap-server localhost:9092 \
  --from-beginning --max-messages 3
```

You should see JSON posts mentioning your brand keyword. Bluesky's volume is lower than Twitter's; for niche keywords like "fyxer" expect minutes between matches. To smoke-test the wiring quickly, temporarily set `BRAND_KEYWORDS=the` and `LANG_FILTER=en` — you'll see hits within seconds, then revert.

---

## Phase 3 — Sentiment Enricher (~30 min)

**Goal:** Read from `bluesky-raw-posts`, run VADER, write to `sentiment-enriched`.

### `src/sentiment_enricher.py`

```python
import json
from confluent_kafka import Consumer, Producer, KafkaError
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from src.config import KAFKA_BOOTSTRAP, TOPIC_RAW, TOPIC_ENRICHED

def label_for(compound: float) -> str:
    if compound >= 0.05: return "positive"
    if compound <= -0.05: return "negative"
    return "neutral"

def main():
    analyzer = SentimentIntensityAnalyzer()
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "sentiment-enricher",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    consumer.subscribe([TOPIC_RAW])

    print("listening on bluesky-raw-posts...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise Exception(msg.error())

            post = json.loads(msg.value())
            scores = analyzer.polarity_scores(post["text"])

            enriched = {
                **post,
                "sentiment_label": label_for(scores["compound"]),
                "sentiment_score": scores["compound"],
                "sentiment_pos": scores["pos"],
                "sentiment_neu": scores["neu"],
                "sentiment_neg": scores["neg"],
            }
            producer.produce(
                TOPIC_ENRICHED,
                key=msg.key(),
                value=json.dumps(enriched).encode(),
            )
            producer.poll(0)
            consumer.commit(msg, asynchronous=False)
            print(f"  {enriched['sentiment_label']:8s} ({scores['compound']:+.2f}) {post['text'][:70]}")
    finally:
        producer.flush()
        consumer.close()

if __name__ == "__main__":
    main()
```

### Concepts learned here

- **Consumer group**: `group.id="sentiment-enricher"` — Kafka tracks offsets for this group separately from any other consumer group. The DuckDB sink will use a different group, meaning both can read every message independently.
- **`auto.offset.reset="earliest"`**: On first run with this group, start from the beginning of the topic. After offsets are committed, this setting is ignored.
- **Manual commit (`enable.auto.commit=False`)**: We commit only _after_ successfully producing the enriched message. If the process crashes mid-message, the next run reprocesses that one message — at-least-once semantics.

### Verification

```bash
make enricher
# in another terminal:
kafka-console-consumer --topic sentiment-enriched \
  --bootstrap-server localhost:9092 --from-beginning --max-messages 3
```

You should see the same posts with sentiment fields appended.

---

## Phase 4 — DuckDB Sink (~20 min)

**Goal:** Read `sentiment-enriched`, persist to DuckDB.

### `src/duckdb_sink.py`

```python
import json
import duckdb
from confluent_kafka import Consumer, KafkaError
from src.config import KAFKA_BOOTSTRAP, TOPIC_ENRICHED, DUCKDB_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS sentiment_events (
    uri              VARCHAR PRIMARY KEY,
    cid              VARCHAR,
    did              VARCHAR,
    text             VARCHAR,
    langs            VARCHAR[],
    created_at       TIMESTAMP,
    time_us          BIGINT,
    sentiment_label  VARCHAR,
    sentiment_score  FLOAT,
    sentiment_pos    FLOAT,
    sentiment_neu    FLOAT,
    sentiment_neg    FLOAT,
    ingested_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

UPSERT = """
INSERT INTO sentiment_events
    (uri, cid, did, text, langs, created_at, time_us,
     sentiment_label, sentiment_score,
     sentiment_pos, sentiment_neu, sentiment_neg)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (uri) DO UPDATE SET
    sentiment_label = excluded.sentiment_label,
    sentiment_score = excluded.sentiment_score;
"""

def main():
    db = duckdb.connect(DUCKDB_PATH)
    db.execute(SCHEMA)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "duckdb-sink",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_ENRICHED])

    print(f"writing to {DUCKDB_PATH}")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF: continue
                raise Exception(msg.error())

            p = json.loads(msg.value())
            db.execute(UPSERT, [
                p["uri"], p["cid"], p["did"], p["text"],
                p.get("langs", []),
                p["created_at"],
                p.get("time_us", 0),
                p["sentiment_label"], p["sentiment_score"],
                p["sentiment_pos"], p["sentiment_neu"], p["sentiment_neg"],
            ])
            consumer.commit(msg, asynchronous=False)
            print(f"  wrote {p['uri'][-12:]} ({p['sentiment_label']})")
    finally:
        consumer.close()
        db.close()

if __name__ == "__main__":
    main()
```

### Concepts learned here

- **Different consumer group, same topic**: `group.id="duckdb-sink"` is independent from `sentiment-enricher`. Both read all messages. Adding a third consumer (e.g., alerting) just means another group ID.
- **Idempotent writes**: The `ON CONFLICT (uri)` upsert means replaying the same message produces the same DB state. Combined with manual offset commit, this gives effectively-once-to-DB semantics even though Kafka delivery is at-least-once.
- **DuckDB casts ISO strings to TIMESTAMP** automatically when the column type is TIMESTAMP — no manual `datetime.fromisoformat` needed.

### Verification

```bash
make sink
# while it runs, in another terminal:
duckdb data/sentiment.duckdb -c "SELECT COUNT(*), MIN(created_at), MAX(created_at) FROM sentiment_events;"
```

---

## Phase 5 — Streamlit Dashboard (~60 min)

**Goal:** Apple-styled dashboard reading from DuckDB.

### `src/dashboard.py`

Skeleton — flesh out incrementally:

```python
import time
from datetime import datetime, timedelta, timezone

import duckdb
import plotly.graph_objects as go
import streamlit as st

from src.config import DUCKDB_PATH, BRAND_KEYWORDS

st.set_page_config(
    page_title="Bluesky Sentiment Monitor",
    page_icon="○",
    layout="wide",
)

st.markdown("""
<style>
  .stApp { background-color: #F5F5F7; }
  body, .stMarkdown, .stMetric {
    font-family: -apple-system, BlinkMacSystemFont, "San Francisco",
                 "Helvetica Neue", sans-serif;
  }
  div[data-testid="metric-container"] {
    background-color: #FFFFFF;
    border: 1px solid #E5E5E9;
    border-radius: 12px;
    padding: 16px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.12);
  }
  div[data-testid="stPlotlyChart"] {
    background-color: #FFFFFF;
    border: 1px solid #E5E5E9;
    border-radius: 12px;
    padding: 16px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.12);
  }
  h1 { font-weight: 700; color: #1D1D1F; }
  h2, h3 { font-weight: 600; color: #1D1D1F; }
</style>
""", unsafe_allow_html=True)

COLORS = {"positive": "#34C759", "neutral": "#8E8E93", "negative": "#FF3B30"}

APPLE_LAYOUT = dict(
    paper_bgcolor="#FFFFFF", plot_bgcolor="#FFFFFF",
    font=dict(family="-apple-system, BlinkMacSystemFont, sans-serif",
              color="#1D1D1F", size=13),
    xaxis=dict(gridcolor="#E5E5E9", linecolor="#E5E5E9"),
    yaxis=dict(gridcolor="#E5E5E9", linecolor="#E5E5E9"),
    margin=dict(l=0, r=0, t=24, b=0),
)

@st.cache_resource
def get_db():
    return duckdb.connect(DUCKDB_PATH, read_only=True)

def query(sql, params=None):
    return get_db().execute(sql, params or []).fetchdf()

# ---------- header ----------
st.markdown(f"# Bluesky Sentiment Monitor — {', '.join(BRAND_KEYWORDS)}")
window_choice = st.selectbox("Window", ["1 hour", "6 hours", "24 hours", "7 days"], index=2)
window_hours = {"1 hour": 1, "6 hours": 6, "24 hours": 24, "7 days": 168}[window_choice]
since = datetime.now(timezone.utc) - timedelta(hours=window_hours)

# ---------- KPI row ----------
kpi = query("""
    SELECT
        COUNT(*) AS total,
        AVG(sentiment_score) AS avg_score,
        SUM(CASE WHEN sentiment_label='positive' THEN 1 ELSE 0 END) AS pos,
        SUM(CASE WHEN sentiment_label='negative' THEN 1 ELSE 0 END) AS neg
    FROM sentiment_events WHERE created_at >= ?
""", [since]).iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Posts", f"{int(kpi.total):,}")
c2.metric("Avg Sentiment", f"{kpi.avg_score:+.2f}" if kpi.total else "—")
c3.metric("Positive", f"{(kpi.pos / kpi.total * 100):.1f}%" if kpi.total else "—")
c4.metric("Negative", f"{(kpi.neg / kpi.total * 100):.1f}%" if kpi.total else "—")

# ---------- time-series ----------
ts = query("""
    SELECT date_trunc('hour', created_at) AS hour,
           AVG(sentiment_score) AS avg_score,
           COUNT(*) AS posts
    FROM sentiment_events WHERE created_at >= ?
    GROUP BY 1 ORDER BY 1
""", [since])

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=ts["hour"], y=ts["avg_score"],
    mode="lines+markers", line=dict(color="#0071E3", width=2),
    marker=dict(size=6),
))
fig.update_layout(**APPLE_LAYOUT, height=320, title="Sentiment Over Time")
st.plotly_chart(fig, use_container_width=True)

# ---------- breakdown row ----------
col_left, col_right = st.columns(2)

with col_left:
    bd = query("""
        SELECT date_trunc('hour', created_at) AS hour, sentiment_label, COUNT(*) AS n
        FROM sentiment_events WHERE created_at >= ?
        GROUP BY 1, 2 ORDER BY 1
    """, [since])
    fig2 = go.Figure()
    for label in ["positive", "neutral", "negative"]:
        d = bd[bd["sentiment_label"] == label]
        fig2.add_trace(go.Bar(x=d["hour"], y=d["n"], name=label.title(),
                              marker_color=COLORS[label]))
    fig2.update_layout(**APPLE_LAYOUT, barmode="stack", height=320,
                       title="Hourly Volume by Sentiment")
    st.plotly_chart(fig2, use_container_width=True)

with col_right:
    authors = query("""
        SELECT did, COUNT(*) AS n, AVG(sentiment_score) AS avg_score
        FROM sentiment_events WHERE created_at >= ?
        GROUP BY 1 ORDER BY n DESC LIMIT 8
    """, [since])
    # Show last 12 chars of DID for readability — could resolve to handle via atproto SDK.
    authors["author"] = authors["did"].str.slice(-12)
    fig3 = go.Figure(go.Bar(
        x=authors["n"], y=authors["author"], orientation="h",
        marker_color="#0071E3",
    ))
    fig3.update_layout(**APPLE_LAYOUT, height=320, title="Top Authors")
    st.plotly_chart(fig3, use_container_width=True)

# ---------- recent posts ----------
st.markdown("### Recent Posts")
recent = query("""
    SELECT created_at, did, text, sentiment_label, sentiment_score, uri
    FROM sentiment_events ORDER BY created_at DESC LIMIT 20
""")
# Build clickable bsky.app link from uri: at://{did}/app.bsky.feed.post/{rkey}
def to_bsky_url(uri: str) -> str:
    parts = uri.split("/")
    if len(parts) < 5: return ""
    did, rkey = parts[2], parts[-1]
    return f"https://bsky.app/profile/{did}/post/{rkey}"
recent["link"] = recent["uri"].apply(to_bsky_url)
st.dataframe(
    recent[["created_at", "did", "text", "sentiment_label", "sentiment_score", "link"]],
    use_container_width=True, hide_index=True,
    column_config={"link": st.column_config.LinkColumn("link")},
)

# ---------- auto-refresh ----------
st.caption(f"Refreshes every 10s · last updated {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")
time.sleep(10)
st.rerun()
```

### Verification

```bash
make dashboard
# opens http://localhost:8501
```

---

## Phase 6 — Run It All Together

You'll need 4 terminals (or use `tmux`). Order matters for the _first_ run only — after that they can start in any order since Kafka buffers.

```bash
# terminal 1 (one-time)
make topics

# terminal 2 — keep running
make producer

# terminal 3 — keep running
make enricher

# terminal 4 — keep running
make sink

# terminal 5 — open browser
make dashboard
```

**What you'll observe:**

1. Producer prints each matched post (slowly, since "fyxer" is a niche keyword).
2. Enricher prints sentiment classifications a fraction of a second later.
3. Sink prints DB writes.
4. Dashboard updates every 10 seconds.

Try shutting down the dashboard and sink for a few minutes — when they restart, they catch up from the last committed offset. Try shutting down the producer and restarting it — it resumes from the persisted Jetstream cursor. **This is Kafka's killer feature**: every component is restartable without data loss.

---

## Phase 7 — Containerisation (~30 min)

**Goal:** Package the whole stack — Kafka broker, producer, enricher, sink, dashboard — into a single Docker image runnable from Docker Desktop. No host-side Java, Homebrew Kafka, or Python venv required after this phase.

### Design choices

- **One container, all processes.** A small startup script formats KRaft storage on first run, launches the broker in the background, polls until it accepts connections, creates topics, then launches the four Python processes. If any process exits, the script tears down the rest so Docker can restart the container as a unit.
- **KRaft single-node.** Broker and controller in the same JVM (`process.roles=broker,controller`). No ZooKeeper. `controller.quorum.voters=1@localhost:9093` since everything is local to the container.
- **Volumes for durability.** `./data` is bind-mounted so the DuckDB file and Jetstream cursor survive `docker compose down`. The Kafka log dir lives in a named volume `kafka-data` so topic offsets and message retention also survive.
- **Network.** Only port 8501 (Streamlit) is published to the host. Kafka stays on `localhost:9092` _inside_ the container — pipeline processes reach it on loopback, the host never needs to.
- **Env vars supersede `.env`.** `config.py` calls `load_dotenv()` which is a no-op inside the container; the Dockerfile sets defaults and `docker-compose.yml` overrides `BRAND_KEYWORDS` / `LANG_FILTER` from the host's environment.

### Files to create

**`Dockerfile`** — `python:3.11-slim` base, adds `openjdk-17-jre-headless`, downloads Kafka 3.9.0 from the Apache archive into `/opt/kafka`, installs `requirements.txt`, copies `src/`, `docker/`, and `.streamlit/`, sets the runtime env vars, exposes 8501, and uses `docker/start.sh` as the entrypoint.

**`docker/kraft.properties`** — single-node KRaft config:

```properties
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=/var/lib/kafka
num.partitions=3
default.replication.factor=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
```

**`docker/start.sh`** — boot sequence:

1. If `/var/lib/kafka/meta.properties` is missing, run `kafka-storage.sh format` with a fresh `random-uuid`.
2. `kafka-server-start.sh` in the background, log to `/var/log/kafka.log`.
3. Poll `kafka-topics.sh --list` until it succeeds (≤60s).
4. `python -m src.kafka_admin` to create the two topics (idempotent — "already exists" errors are ignored).
5. Launch `bluesky_producer`, `sentiment_enricher`, `duckdb_sink`, and `streamlit run src/dashboard.py` in the background.
6. `wait -n` — exit when any child dies, then kill the rest.

**`docker-compose.yml`** — wires the bind mount, named volume, port, and the two host-overridable env vars:

```yaml
services:
  bluesky-sentiment:
    build: .
    image: bluesky-sentiment:latest
    container_name: bluesky-sentiment
    ports:
      - "8501:8501"
    environment:
      BRAND_KEYWORDS: ${BRAND_KEYWORDS:-apple}
      LANG_FILTER: ${LANG_FILTER:-en}
    volumes:
      - ./data:/app/data
      - kafka-data:/var/lib/kafka
    restart: unless-stopped

volumes:
  kafka-data:
```

**`.dockerignore`** — excludes `.venv`, `data/`, `docs/`, `__pycache__`, `.git`, `.streamlit/secrets.toml`, `README.md`, `Makefile`. Keeps the build context small and prevents the host's DuckDB file from being baked into the image.

### Concepts learned here

- **Process supervision in a container.** A bash script with `wait -n` is the simplest viable supervisor when "container exits = Docker restarts everything" is the desired behaviour. Heavier alternatives (supervisord, s6-overlay) make sense once you want per-process restart policies.
- **KRaft formatting is a one-time operation.** The `meta.properties` sentinel makes the entrypoint idempotent across `docker compose up` cycles — formatting only runs on a fresh `kafka-data` volume.
- **`localhost` inside a container** is the container's own loopback, not the host's. That's exactly what we want here: the four Python processes reach Kafka on `localhost:9092` without any service discovery. Only the dashboard needs to escape the network namespace, via the published 8501 port.
- **Bind mount vs named volume.** `./data` is a bind mount so you can poke at the DuckDB file from the host (e.g. `duckdb data/sentiment.duckdb -c "..."`). Kafka's log dir is a named volume because it's an implementation detail you never want to inspect from the host.

### Verification

```bash
docker compose build
docker compose up
# logs stream to the terminal; first boot takes ~20s while Kafka formats storage
# open http://localhost:8501
```

To change the tracked keyword without rebuilding:

```bash
BRAND_KEYWORDS=openai docker compose up
```

To wipe everything and start clean:

```bash
docker compose down -v   # removes the kafka-data volume too
rm -rf data/*.duckdb data/jetstream_cursor.txt
```

---

## Validation Checklist

Before declaring done:

- [ ] `kafka-topics --list` shows `bluesky-raw-posts` and `sentiment-enriched`
- [ ] Producer connects to Jetstream and prints at least one matched post (broaden `BRAND_KEYWORDS` temporarily if waiting too long)
- [ ] `kafka-console-consumer --topic bluesky-raw-posts` shows JSON
- [ ] `kafka-console-consumer --topic sentiment-enriched` shows JSON with `sentiment_*` fields
- [ ] `duckdb data/sentiment.duckdb -c "SELECT COUNT(*) FROM sentiment_events"` returns > 0
- [ ] `data/jetstream_cursor.txt` exists and updates over time
- [ ] Dashboard renders without errors at http://localhost:8501
- [ ] Killing the producer mid-stream and restarting it does not produce duplicate URIs in DuckDB (idempotent upsert)
- [ ] Stopping/restarting the enricher resumes from last offset
- [ ] `docker compose build` succeeds with no warnings
- [ ] `docker compose up` brings the dashboard up at http://localhost:8501 within ~30s of a cold start
- [ ] `docker compose down` followed by `docker compose up` retains existing posts (DuckDB and Kafka volumes persist)

---

## Known Limitations of This MVP (Future Work)

- **No engagement signals** — likes, reposts, and replies are separate Jetstream collections. Folding them in means subscribing to `app.bsky.feed.like` / `repost` and joining on `subject.uri`.
- **DIDs not resolved to handles** — the dashboard shows DID suffixes. Add `app.bsky.actor.getProfile` lookups via the `atproto` SDK, cached in DuckDB, for human-readable names.
- **English-only** — VADER lexicon is English. The pipeline already filters by `LANG_FILTER=en`; multi-language support requires the transformer upgrade.
- **VADER misses sarcasm and AI/SaaS slang.** Upgrade to `cardiffnlp/twitter-roberta-base-sentiment-latest` is a 5-line change in `sentiment_enricher.py`.
- **No backfill** — only forward-streaming posts are captured. For historical mentions on cold-start, add a one-shot script using `atproto`'s `app.bsky.feed.searchPosts`.
- **Single broker, single replica** — fine for local; would not survive a node failure.

---

## Estimated Total Time

| Phase                    | Time         |
| ------------------------ | ------------ |
| 0 — Scaffolding          | 15 min       |
| 1 — Shared modules       | 20 min       |
| 2 — Bluesky producer     | 30 min       |
| 3 — Sentiment enricher   | 30 min       |
| 4 — DuckDB sink          | 20 min       |
| 5 — Dashboard            | 60 min       |
| 6 — Integration & polish | 30 min       |
| 7 — Containerisation     | 30 min       |
| **Total**                | **~4 hours** |
