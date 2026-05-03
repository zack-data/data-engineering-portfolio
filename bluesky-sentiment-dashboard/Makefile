.PHONY: topics producer enricher sink dashboard clean

PYTHON ?= python3

topics:
	$(PYTHON) -m src.kafka_admin

producer:
	$(PYTHON) -m src.bluesky_producer

enricher:
	$(PYTHON) -m src.sentiment_enricher

sink:
	$(PYTHON) -m src.duckdb_sink

dashboard:
	PYTHONPATH=. streamlit run src/dashboard.py

clean:
	rm -rf data/*.duckdb data/jetstream_cursor.txt __pycache__ src/__pycache__
