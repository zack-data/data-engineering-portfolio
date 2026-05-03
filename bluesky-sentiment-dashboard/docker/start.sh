#!/usr/bin/env bash
set -uo pipefail

KAFKA_DATA=/var/lib/kafka
KAFKA_CONFIG=/app/docker/kraft.properties

# Format KRaft storage on first run
if [ ! -f "$KAFKA_DATA/meta.properties" ]; then
  echo "[start] formatting kraft storage"
  CLUSTER_ID=$(kafka-storage.sh random-uuid)
  kafka-storage.sh format -t "$CLUSTER_ID" -c "$KAFKA_CONFIG"
fi

echo "[start] starting kafka broker"
kafka-server-start.sh "$KAFKA_CONFIG" > /var/log/kafka.log 2>&1 &
KAFKA_PID=$!

echo "[start] waiting for kafka to accept connections"
for _ in $(seq 1 60); do
  if kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    echo "[start] kafka is up"
    break
  fi
  sleep 1
done

echo "[start] creating topics"
python -m src.kafka_admin || true

echo "[start] launching pipeline"
python -m src.bluesky_producer &
PRODUCER_PID=$!
python -m src.sentiment_enricher &
ENRICHER_PID=$!
python -m src.duckdb_sink &
SINK_PID=$!
streamlit run src/dashboard.py \
    --server.port=8501 \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false &
DASH_PID=$!

# If any process exits, tear down the rest so Docker restarts the container.
wait -n
echo "[start] a process exited; shutting down"
kill "$KAFKA_PID" "$PRODUCER_PID" "$ENRICHER_PID" "$SINK_PID" "$DASH_PID" 2>/dev/null || true
exit 1
