import json

from confluent_kafka import Consumer, Producer, KafkaError
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from src.config import KAFKA_BOOTSTRAP, TOPIC_RAW, TOPIC_ENRICHED


def label_for(compound: float) -> str:
    if compound >= 0.05:
        return "positive"
    if compound <= -0.05:
        return "negative"
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
