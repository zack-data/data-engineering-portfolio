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
