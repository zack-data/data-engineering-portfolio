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
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
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
            db.execute("CHECKPOINT")
            consumer.commit(msg, asynchronous=False)
            print(f"  wrote {p['uri'][-12:]} ({p['sentiment_label']})")
    finally:
        consumer.close()
        db.close()


if __name__ == "__main__":
    main()
