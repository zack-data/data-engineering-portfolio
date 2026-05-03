# Bluesky Brand Sentiment Dashboard — Architecture Research

> A beginner-friendly guide to building a real-time sentiment pipeline with Kafka, the Bluesky Jetstream firehose, and Streamlit. Written to explain _why_ each technology was chosen, not just what to use.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Kafka — What It Is and Why It's Here](#2-kafka--what-it-is-and-why-its-here)
3. [Bluesky Data Ingestion (AT Protocol + Jetstream)](#3-bluesky-data-ingestion-at-protocol--jetstream)
4. [Sentiment Analysis](#4-sentiment-analysis)
5. [Data Storage — DuckDB](#5-data-storage--duckdb)
6. [Dashboard — Streamlit](#6-dashboard--streamlit)
7. [Apple Design System Reference](#7-apple-design-system-reference)
8. [Technology Stack Summary](#8-technology-stack-summary)
9. [Local Setup Prerequisites](#9-local-setup-prerequisites)

---

## 1. System Overview

The pipeline has four stages:

```
Bluesky Jetstream (WebSocket)
    │
    │  raw post JSON
    ▼
Kafka Topic: bluesky-raw-posts
    │
    │  consumer polls messages
    ▼
Sentiment Enrichment Service (VADER)
    │
    │  enriched post + sentiment score
    ▼
Kafka Topic: sentiment-enriched
    │
    │  consumer writes to DB
    ▼
DuckDB (local file: sentiment.duckdb)
    │
    │  SQL queries
    ▼
Streamlit Dashboard (localhost:8501)
```

**Each arrow in this diagram is important.** Nothing is coupled directly — the Bluesky ingester doesn't know the sentiment service exists, and the dashboard doesn't know Kafka exists. They only share a contract: JSON message schemas on Kafka topics and a DuckDB schema. This is the key architectural benefit of using a message broker.

---

## 2. Kafka — What It Is and Why It's Here

### The core problem Kafka solves

Without Kafka, you'd have a Python script that reads the Bluesky firehose, runs sentiment analysis, writes to a database, and the dashboard reads from that database. This works, but:

- Everything is tightly coupled — a bug in sentiment analysis crashes ingestion
- You can't replay data if the sentiment model changes
- You can't add a second consumer (e.g., alerting) without modifying the ingester
- Throughput is bottlenecked by the slowest step

This matters even more on Bluesky than on Reddit: the firehose pushes thousands of events per second globally, so a slow downstream step would force you to drop messages on the floor. Kafka decouples producers from consumers using a **durable, ordered log of messages**. The ingester writes filtered posts to Kafka and moves on; consumers read at their own pace.

### Key concepts you need to understand

**Topic**
A named channel for a category of messages. Think of it like a database table, but append-only. You'll have two topics:

- `bluesky-raw-posts` — brand-matched posts pulled from the Jetstream firehose
- `sentiment-enriched` — posts with sentiment scores appended

**Partition**
Each topic is split into partitions for parallelism. Partition 0, 1, 2, etc. Messages with the same key (e.g., the post's `at-uri`) always go to the same partition. Within a partition, messages are strictly ordered by offset. Across partitions, ordering is not guaranteed.

```
bluesky-raw-posts
├── Partition 0: [msg@offset0, msg@offset1, msg@offset2, ...]
├── Partition 1: [msg@offset0, msg@offset1, ...]
└── Partition 2: [msg@offset0, msg@offset1, ...]
```

**Offset**
A monotonically increasing integer that marks your position in a partition. When a consumer processes a message, it commits its offset. If it crashes and restarts, it resumes from the last committed offset — no data loss.

**Producer**
A process that writes messages to a topic. Your Bluesky ingester is a producer. It serializes a post as JSON and calls `producer.produce(topic, key, value)`.

**Consumer**
A process that reads messages from a topic. Your sentiment enrichment service is a consumer of `bluesky-raw-posts` and also a producer of `sentiment-enriched`. The DuckDB sink is a consumer of `sentiment-enriched`.

**Consumer Group**
A set of consumers that collectively read a topic. Each partition is assigned to exactly one consumer in the group. If you have 3 partitions and 3 consumers in a group, each consumer handles one partition in parallel. Consumer groups enable horizontal scaling without any code changes.

```
Consumer Group: sentiment-enricher
├── Consumer A → handles Partition 0
├── Consumer B → handles Partition 1
└── Consumer C → handles Partition 2
```

**Broker**
A Kafka server process. For local development, you run one broker on `localhost:9092`. In production, you'd run 3+ brokers for fault tolerance.

**KRaft Mode (no ZooKeeper)**
Before Kafka 4.0, Kafka required a separate ZooKeeper cluster to manage metadata. Kafka 4.0 (March 2025) removed ZooKeeper entirely; KRaft (Kafka Raft) replaces it with an embedded consensus protocol. You'll use KRaft mode — one process, one config file, much simpler local setup.

### Why Kafka for this project specifically

This project is deliberately using Kafka as a learning vehicle. The practical benefits here are:

1. The Bluesky firehose is firehose-shaped — bursts of activity around news cycles can briefly outpace your sentiment model. Kafka buffers them so the consumer processes at its own steady pace, rather than dropping events.
2. If you upgrade from VADER to a transformer model (which is slower), only the consumer changes — no pipeline rewiring.
3. The dashboard can be restarted and rebuilt without losing any events — they're still in the Kafka topic.
4. You learn producer/consumer patterns that transfer directly to production data engineering jobs.

---

## 3. Bluesky Data Ingestion (AT Protocol + Jetstream)

### Why Bluesky, and what's different from Reddit

Bluesky is built on the **AT Protocol** (Authenticated Transfer Protocol). A few characteristics that change the ingestion design:

- **Public firehose by default.** Every post on the network is broadcast as a public stream. There is no per-keyword search subscription — you consume _everything_ and filter client-side.
- **No "subreddit" concept.** Posts have an author, text, language tags, and timestamps. Community segmentation comes from custom feeds, follows, and labels — not subforums.
- **No rate limits on the firehose.** Unlike Reddit's 60 req/min OAuth ceiling, Jetstream is a long-lived WebSocket — you stay connected and receive events as they happen.
- **Post identity is an `at-uri`**, e.g. `at://did:plc:abc123.../app.bsky.feed.post/3kg2...`. This becomes your Kafka message key.

### Jetstream vs the raw firehose

Bluesky exposes two real-time streams:

|                        | Jetstream                    | Raw firehose (`com.atproto.sync.subscribeRepos`) |
| ---------------------- | ---------------------------- | ------------------------------------------------ |
| Format                 | JSON over WebSocket          | Binary CBOR + CAR files                          |
| Auth                   | None                         | None                                             |
| Filterable server-side | Yes (collection, DID)        | No                                               |
| Throughput             | Same events, lighter payload | Full repository commits                          |
| Best for               | App-layer consumers (us)     | Indexers, mirrors, relays                        |

**Jetstream is the right choice for this project.** It's operated by Bluesky and serves filtered JSON events directly, eliminating the need to parse CBOR/CAR. Public endpoints include:

- `wss://jetstream1.us-east.bsky.network/subscribe`
- `wss://jetstream2.us-east.bsky.network/subscribe`
- `wss://jetstream1.us-west.bsky.network/subscribe`
- `wss://jetstream2.us-west.bsky.network/subscribe`

You can request a server-side filter via query string, e.g. only commits to the post collection:

```
wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post
```

### Python clients

|                                            | `websockets` (stdlib-style async) | `atproto` SDK                          |
| ------------------------------------------ | --------------------------------- | -------------------------------------- |
| Jetstream consumption                      | Direct, minimal deps              | Built-in firehose helpers              |
| Authenticated API calls (search, getPosts) | Manual                            | Yes                                    |
| Maintenance                                | Mature, ubiquitous                | Active, the official-ish Python client |
| Learning curve                             | Gentle                            | Gentle                                 |

For ingestion-only, plain `websockets` is enough. Add `atproto` if you later want to fetch like counts, search post history, or resolve handles to DIDs.

### Data fields worth capturing

Each Jetstream commit on `app.bsky.feed.post` looks roughly like:

```json
{
  "did": "did:plc:abc123...",
  "time_us": 1746201234567890,
  "kind": "commit",
  "commit": {
    "operation": "create",
    "collection": "app.bsky.feed.post",
    "rkey": "3kg2hf7tyq2sq",
    "record": {
      "$type": "app.bsky.feed.post",
      "text": "I love using Fyxer to clean up my inbox",
      "createdAt": "2026-05-02T14:32:00.123Z",
      "langs": ["en"]
    },
    "cid": "bafyreigh..."
  }
}
```

Fields you'll persist:

```
Post-level fields
├── uri              → at://{did}/{collection}/{rkey} — Kafka message key
├── cid              → content hash (dedup / idempotency aid)
├── did              → author DID
├── text             → primary sentiment signal
├── created_at       → ISO timestamp from the record
├── langs            → array of BCP-47 language codes (filter to ["en"] for VADER)
└── ingested_at      → wall-clock time when our ingester saw it
```

**`created_at` vs `ingested_at`**: Always use `record.createdAt` as your event timestamp for time-series charts. `ingested_at` is useful for measuring pipeline lag.

**No score/num_comments at firehose time.** Likes, reposts, and replies arrive as _separate_ events on their own collections (`app.bsky.feed.like`, `app.bsky.feed.repost`). For an MVP we ignore engagement counts; future work could subscribe to those collections and fold them in.

### Streaming approach

Conceptual ingester loop (implementation lives in `src/bluesky_producer.py`):

```python
import asyncio, json, websockets
from confluent_kafka import Producer

JETSTREAM_URL = (
    "wss://jetstream2.us-east.bsky.network/subscribe"
    "?wantedCollections=app.bsky.feed.post"
)
BRAND_KEYWORDS = ["fyxer"]

def matches_brand(text: str) -> bool:
    text = text.lower()
    return any(kw in text for kw in BRAND_KEYWORDS)

async def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    async with websockets.connect(JETSTREAM_URL) as ws:
        async for raw in ws:
            evt = json.loads(raw)
            commit = evt.get("commit") or {}
            if commit.get("operation") != "create":
                continue
            record = commit.get("record") or {}
            text = record.get("text", "")
            if not matches_brand(text):
                continue
            uri = f"at://{evt['did']}/{commit['collection']}/{commit['rkey']}"
            payload = {
                "uri": uri,
                "cid": commit.get("cid"),
                "did": evt["did"],
                "text": text,
                "created_at": record.get("createdAt"),
                "langs": record.get("langs", []),
            }
            producer.produce(
                "bluesky-raw-posts",
                key=uri.encode(),
                value=json.dumps(payload).encode(),
            )
            producer.poll(0)

asyncio.run(main())
```

**Reconnect strategy**: WebSockets drop. Wrap the `connect` call in a retry loop with exponential backoff. Jetstream supports a `cursor` query parameter so you can resume from the last `time_us` you processed — store that in a small file or in DuckDB so a restart doesn't lose recent events.

**Language filtering**: VADER is English-only. Filter to `"en" in langs` before matching keywords; otherwise you'll score French and Japanese posts on an English lexicon and produce noise.

### Brand monitoring on Bluesky vs Reddit

Reddit gives you communities (`r/Fyxer`) — a built-in form of audience targeting. Bluesky doesn't. Two ways to compensate:

1. **Keyword filter on the global firehose** (what we do above). Catches every public mention regardless of who posts it.
2. **Authenticated search**, via `app.bsky.feed.searchPosts` from the `atproto` SDK. Useful as a backfill on cold-start so the dashboard isn't empty for the first hour, but it's a polled REST endpoint (not a stream) and is rate-limited.

The MVP relies on (1); (2) is listed in future work as an optional backfill.

---

## 4. Sentiment Analysis

#### VADER (recommended for MVP)

VADER (Valence Aware Dictionary and sEntiment Reasoner) is a rule-based lexicon tuned explicitly for social media text. It handles:

- ALL CAPS for emphasis (`LOVE this` scores higher than `love this`)
- Emoticons (`:)` adds positive signal)
- Negations (`not good` → negative)
- Intensifiers (`really bad` vs `bad`)

**Output**: A compound score from -1.0 (most negative) to +1.0 (most positive), plus separate pos/neu/neg proportions.

**Latency**: < 1ms per post. Can process 10,000+ posts/second on a single core — comfortably faster than the Bluesky firehose even when you remove keyword filtering.

**Weakness**: Lexicon-based — it can't learn. Sarcasm and domain-specific slang can fool it. English-only, so filter `langs` upstream.

```python
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()
scores = analyzer.polarity_scores("Fyxer just sorted 200 emails for me, magical")
# → {'neg': 0.0, 'neu': 0.583, 'pos': 0.417, 'compound': 0.6486}
```

Threshold convention: compound ≥ 0.05 → positive, ≤ -0.05 → negative, else neutral.

### Recommended labeling thresholds

| Compound Score | Label    | Color     |
| -------------- | -------- | --------- |
| ≥ 0.05         | Positive | `#34C759` |
| -0.05 to 0.05  | Neutral  | `#8E8E93` |
| ≤ -0.05        | Negative | `#FF3B30` |

---

## 5. Data Storage — DuckDB

### Why not SQLite?

SQLite is row-oriented and OLTP-shaped. Sentiment dashboards are analytical — they aggregate millions of rows across time ranges:

```sql
SELECT DATE_TRUNC('hour', created_at), AVG(sentiment_score), COUNT(*)
FROM sentiment_events
WHERE created_at > NOW() - INTERVAL 24 HOURS
GROUP BY 1
ORDER BY 1;
```

On 500k rows, this query takes ~45 seconds on SQLite. On DuckDB, < 0.3 seconds. DuckDB is column-oriented and vectorized — it reads only the `created_at` and `sentiment_score` columns from disk, skipping everything else, and processes them in SIMD batches.

### DuckDB is a single file

`duckdb.connect('sentiment.duckdb')` opens (or creates) a file in your working directory. No server process, no ports, no config — the right fit for a local portfolio project.

### Proposed schema

```sql
-- Raw event stream (one row per Bluesky post)
CREATE TABLE sentiment_events (
    uri              VARCHAR PRIMARY KEY,    -- at://{did}/app.bsky.feed.post/{rkey}
    cid              VARCHAR,
    did              VARCHAR,                -- author DID
    text             VARCHAR,
    langs            VARCHAR[],
    created_at       TIMESTAMP,              -- record.createdAt
    ingested_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sentiment_label  VARCHAR,                -- 'positive', 'negative', 'neutral'
    sentiment_score  FLOAT,                  -- VADER compound: -1.0 to +1.0
    sentiment_pos    FLOAT,
    sentiment_neu    FLOAT,
    sentiment_neg    FLOAT
);

-- Pre-aggregated hourly rollups for fast dashboard queries
CREATE TABLE sentiment_hourly (
    hour            TIMESTAMP,
    lang            VARCHAR,
    positive_count  INTEGER,
    neutral_count   INTEGER,
    negative_count  INTEGER,
    avg_score       FLOAT,
    total_posts     INTEGER,
    PRIMARY KEY (hour, lang)
);
```

The `sentiment_hourly` table is populated by the consumer's windowing logic and makes the dashboard's time-series charts instantaneous even when `sentiment_events` grows to millions of rows. Where the Reddit version segmented by subreddit, the Bluesky version segments by language code — the most useful built-in axis the firehose exposes. Author-level aggregation can be added later by joining on `did`.

---

## 6. Dashboard — Streamlit

### Why Streamlit over Dash or FastAPI+JS

|                           | Streamlit                  | Dash                    | FastAPI + JS  |
| ------------------------- | -------------------------- | ----------------------- | ------------- |
| Time to working dashboard | ~2 hours                   | ~1 day                  | ~3 days       |
| Python-only               | Yes                        | Yes (HTML/CSS optional) | No (need JS)  |
| Real-time updates         | Polling via `st.rerun()`   | Callbacks               | WebSockets    |
| Custom CSS                | Possible via `st.markdown` | Easy                    | Total control |
| Apple aesthetic           | Achievable                 | Achievable              | Total control |

This project's goal is learning Kafka. Streamlit keeps dashboard complexity low so you can focus on the streaming pipeline.

### Real-time update pattern

Streamlit supports polling with `time.sleep` inside a loop, combined with `st.rerun()`:

```python
import streamlit as st
import time

placeholder = st.empty()

while True:
    data = query_duckdb()
    with placeholder.container():
        render_charts(data)
    time.sleep(5)
    st.rerun()
```

This avoids a persistent Kafka consumer in the dashboard process — the dashboard just reads DuckDB. The Kafka consumer is a separate process writing to DuckDB.

### Recommended dashboard layout

```
┌────────────────────────────────────────────────────────────┐
│  Sentiment Monitor          [keyword:] [24h ▾]             │
├──────────────┬──────────────┬──────────────┬───────────────┤
│  Overall     │  Positive    │  Negative    │  Post Volume  │
│  Score       │  Posts       │  Posts       │  (last 24h)   │
│  +0.42       │  1,203       │  287         │  1,891        │
│  ▲ +0.06     │  63.6%       │  15.2%       │  ▲ +12%       │
├──────────────┴──────────────┴──────────────┴───────────────┤
│  Sentiment Over Time (rolling 1h average)                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  [line chart: positive/neutral/negative bands]      │   │
│  └─────────────────────────────────────────────────────┘   │
├───────────────────────┬────────────────────────────────────┤
│  Breakdown by Hour    │  Top Authors                       │
│  [stacked bar chart]  │  [horizontal bar chart, by handle] │
├───────────────────────┴────────────────────────────────────┤
│  Recent Posts                                              │
│  [table: text | author | sentiment | time | link]          │
└────────────────────────────────────────────────────────────┘
```

**Metric cards** (row 1) — overall score, post counts, volume delta
**Time-series** (row 2) — rolling 1h average sentiment across the selected window
**Breakdown + authors** (row 3) — hourly stacked bar + most-mentioning authors (resolved from DID via `app.bsky.actor.getProfile` if you want handles instead of raw DIDs)
**Recent posts** (row 4) — live feed of the last 20 posts with inline sentiment badges. Each row links to `https://bsky.app/profile/{did}/post/{rkey}` so you can click through.

---

## 7. Design System Reference

### Color palette

| Token          | Hex       | Usage                        |
| -------------- | --------- | ---------------------------- |
| Background     | `#F5F5F7` | Page background              |
| Surface        | `#FFFFFF` | Cards, elevated panels       |
| Text Primary   | `#1D1D1F` | Headlines, body              |
| Text Secondary | `#86868B` | Labels, metadata, timestamps |
| Text Tertiary  | `#A2A2A7` | Hints, disabled              |
| Border         | `#E5E5E9` | Card borders, dividers       |
| Accent Blue    | `#0071E3` | CTAs, links, active states   |
| System Green   | `#34C759` | Positive sentiment           |
| System Red     | `#FF3B30` | Negative sentiment           |
| System Orange  | `#FF9500` | Neutral / warning            |
| System Gray    | `#8E8E93` | Neutral sentiment secondary  |

### Typography

We want to use SF Pro. In a browser or Streamlit context, use the system font stack which resolves to SF Pro on macOS:

```css
font-family:
  -apple-system, BlinkMacSystemFont, "San Francisco", "Helvetica Neue",
  sans-serif;
```

| Element             | Size | Weight         | Color     |
| ------------------- | ---- | -------------- | --------- |
| Dashboard title     | 28px | 700 (Bold)     | `#1D1D1F` |
| Section heading     | 18px | 600 (Semibold) | `#1D1D1F` |
| Metric value        | 32px | 700 (Bold)     | varies    |
| Metric label        | 13px | 400 (Regular)  | `#86868B` |
| Body / table        | 15px | 400 (Regular)  | `#1D1D1F` |
| Caption / timestamp | 12px | 400 (Regular)  | `#A2A2A7` |

### Spacing (8pt grid)

All spacing should be a multiple of 8:

| Use                      | Value |
| ------------------------ | ----- |
| Card internal padding    | 16px  |
| Gap between cards        | 24px  |
| Section spacing          | 32px  |
| Chart height (main)      | 320px |
| Chart height (secondary) | 220px |

### Border radius

We want continuous curvature corners. Approximate with:

| Component            | Border Radius |
| -------------------- | ------------- |
| Buttons, badges      | 8px           |
| Cards, inputs        | 12px          |
| Modals, large panels | 16px          |

### Shadow

Subtle elevation-based shadows only:

```css
/* Level 1 — cards */
box-shadow:
  0 1px 3px rgba(0, 0, 0, 0.12),
  0 1px 2px rgba(0, 0, 0, 0.06);

/* Level 2 — elevated overlays */
box-shadow:
  0 4px 6px rgba(0, 0, 0, 0.07),
  0 2px 4px rgba(0, 0, 0, 0.05);
```

### Streamlit theme config

`.streamlit/config.toml`:

```toml
[theme]
base = "light"
primaryColor = "#0071E3"
backgroundColor = "#F5F5F7"
secondaryBackgroundColor = "#FFFFFF"
textColor = "#1D1D1F"
font = "sans serif"
```

### CSS overrides via `st.markdown`

```python
st.markdown("""
<style>
  body, .stMarkdown {
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
    padding: 8px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.12);
  }
  .stApp { background-color: #F5F5F7; }
</style>
""", unsafe_allow_html=True)
```

### Plotly theme

```python
import plotly.graph_objects as go

CHART_LAYOUT = dict(
    paper_bgcolor="#FFFFFF",
    plot_bgcolor="#FFFFFF",
    font=dict(
        family="-apple-system, BlinkMacSystemFont, 'Helvetica Neue', sans-serif",
        color="#1D1D1F",
        size=13,
    ),
    xaxis=dict(gridcolor="#E5E5E9", linecolor="#E5E5E9",
               tickcolor="#86868B", tickfont=dict(color="#86868B", size=12)),
    yaxis=dict(gridcolor="#E5E5E9", linecolor="#E5E5E9",
               tickcolor="#86868B", tickfont=dict(color="#86868B", size=12)),
    margin=dict(l=0, r=0, t=24, b=0),
)

SENTIMENT_COLORS = {
    "positive": "#34C759",
    "neutral":  "#8E8E93",
    "negative": "#FF3B30",
}
```

---

## 8. Technology Stack Summary

| Layer                   | Technology                  | Why                                                                    |
| ----------------------- | --------------------------- | ---------------------------------------------------------------------- |
| Bluesky ingestion       | `websockets` (Jetstream WS) | No auth, no rate limit, JSON-native firehose                           |
| Optional Bluesky API    | `atproto` SDK               | For backfill via `searchPosts` and DID→handle resolution               |
| Message broker          | Apache Kafka 4.x (KRaft)    | Decoupled pipeline, durable event log, real learning value             |
| Python Kafka client     | confluent-kafka 2.x         | Industry standard, librdkafka performance                              |
| Sentiment analysis (v1) | VADER (vaderSentiment 3.3+) | Sub-millisecond latency, social-media tuned, no GPU                    |
| Sentiment analysis (v2) | cardiffnlp/twitter-roberta  | Upgrade path for higher accuracy when needed                           |
| Analytics store         | DuckDB                      | 10–100x faster time-series aggregations than SQLite; zero server setup |
| Dashboard               | Streamlit 1.x               | Fastest path to working UI; Kafka is the learning focus                |
| Charts                  | Plotly                      | Interactive, clean defaults, good Streamlit integration                |

---

## 9. Local Setup Prerequisites

Before building, these must be installed on your Mac:

```bash
# 1. Homebrew (if not already)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# 2. Java (required by Kafka)
brew install openjdk@17
echo 'export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"' >> ~/.zshrc

# 3. Kafka (KRaft mode by default on Homebrew's current formula)
brew install kafka

# 4. Python 3.11+
brew install python@3.11

# 5. Python dependencies
pip install websockets atproto confluent-kafka vaderSentiment duckdb streamlit plotly python-dotenv
```

**Kafka service management:**

```bash
brew services start kafka     # start broker (persists across reboots)
brew services stop kafka
brew services restart kafka

kafka-topics --list --bootstrap-server localhost:9092
```

**Create the two topics you'll need:**

```bash
kafka-topics --create \
  --topic bluesky-raw-posts \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092

kafka-topics --create \
  --topic sentiment-enriched \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

**Smoke-test Jetstream without Kafka** (sanity check that you can reach the firehose):

```bash
# requires `websocat` from Homebrew: brew install websocat
websocat 'wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post' \
  | head -n 5
```

You should see five JSON commit events scroll by within a couple of seconds. If not, check your network — Jetstream has no auth and is reachable from any client.

---

_Research compiled 2026-05-02. Library and protocol versions reflect latest stable at time of writing._
