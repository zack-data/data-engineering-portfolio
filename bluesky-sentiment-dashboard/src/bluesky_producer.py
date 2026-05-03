import asyncio
import json
import pathlib

import websockets
from confluent_kafka import Producer

from src.config import (
    JETSTREAM_URL,
    KAFKA_BOOTSTRAP,
    BRAND_KEYWORDS,
    LANG_FILTER,
    JETSTREAM_CURSOR_PATH,
    TOPIC_RAW,
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
    posts_seen = 0
    matches = 0
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

            posts_seen += 1
            if posts_seen % 1000 == 0:
                print(f"  [heartbeat] saw {posts_seen} posts, {matches} matches")

            time_us = evt.get("time_us", 0)
            if time_us - last_save > 5_000_000:
                save_cursor(time_us)
                last_save = time_us

            if LANG_FILTER and LANG_FILTER not in langs:
                continue
            if not matches_brand(text):
                continue

            matches += 1

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
