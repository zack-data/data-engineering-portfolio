FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    KAFKA_VERSION=3.9.2 \
    SCALA_VERSION=2.13 \
    KAFKA_HOME=/opt/kafka \
    PATH=/opt/kafka/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        curl \
        ca-certificates \
        procps \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL \
        --retry 5 --retry-delay 5 --retry-all-errors \
        --connect-timeout 30 --max-time 600 \
        "https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
        -o /tmp/kafka.tgz \
    && tar -xzf /tmp/kafka.tgz -C /opt \
    && mv "/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" /opt/kafka \
    && rm /tmp/kafka.tgz

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY docker ./docker
COPY .streamlit ./.streamlit

RUN mkdir -p /app/data /var/lib/kafka \
    && chmod +x /app/docker/start.sh

ENV JETSTREAM_URL=wss://jetstream2.us-east.bsky.network/subscribe \
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
    BRAND_KEYWORDS=apple \
    LANG_FILTER=en \
    DUCKDB_PATH=/app/data/sentiment.duckdb \
    JETSTREAM_CURSOR_PATH=/app/data/jetstream_cursor.txt

EXPOSE 8501

ENTRYPOINT ["/app/docker/start.sh"]
