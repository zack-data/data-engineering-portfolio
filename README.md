# Data Engineering Portfolio — Zack Williams

Data Engineer with experience across AI SaaS, retail banking, and cloud-native infrastructure. This repo is a collection of projects I've built outside of work to explore new tools and techniques.

## [Full CV](CV.md) &nbsp;·&nbsp; [LinkedIn](https://www.linkedin.com/in/zack-r-williams/)

## Projects

### [Bluesky Sentiment Dashboard](bluesky-sentiment-dashboard/)

Real-time sentiment pipeline for Bluesky posts. Streams the public Jetstream firehose via Kafka, scores posts with VADER, persists to DuckDB, and surfaces results in a Streamlit dashboard.

![Kafka](https://img.shields.io/badge/Kafka-231F20?style=flat&logo=apache-kafka&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=flat&logo=duckdb&logoColor=black)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat&logo=streamlit&logoColor=white)

| Concept             | Implementation                      |
| ------------------- | ----------------------------------- |
| Streaming ingestion | Kafka + Bluesky Jetstream WebSocket |
| Enrichment          | VADER sentiment scoring             |
| Storage             | DuckDB (local, embedded)            |
| Visualisation       | Streamlit                           |

### [Weather Dashboard](weather-dashboard/)

Apache Airflow pipeline that pulls 7-day forecasts from Open-Meteo, persists them to DuckDB, and visualises them in a Streamlit dashboard. Cities are config-driven and the load step upserts on `(city, forecast_date)` so reruns are idempotent.

![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=apache-airflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=flat&logo=duckdb&logoColor=black)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat&logo=streamlit&logoColor=white)

| Concept       | Implementation                                           |
| ------------- | -------------------------------------------------------- |
| Orchestration | Airflow DAG (`@hourly`) wrapping pure ETL functions      |
| Extraction    | Open-Meteo forecast API (no auth)                        |
| Storage       | DuckDB with idempotent upsert on `(city, forecast_date)` |
| Quality check | Row-count assertion on forecast horizon                  |
| Visualisation | Streamlit                                                |

---

## Stack

Technologies used across this portfolio:

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-231F20?style=flat&logo=apache-kafka&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=apache-airflow&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=flat&logo=duckdb&logoColor=black)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat&logo=streamlit&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat&logo=mysql&logoColor=white)
