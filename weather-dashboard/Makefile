.PHONY: install airflow trigger dashboard test smoke clean

PYTHON ?= python3.11
AIRFLOW_HOME := $(PWD)/airflow

install:
	$(PYTHON) -m pip install -r requirements.txt \
		--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"

airflow:
	AIRFLOW_HOME=$(AIRFLOW_HOME) \
	AIRFLOW__CORE__DAGS_FOLDER=$(PWD)/dags \
	AIRFLOW__CORE__LOAD_EXAMPLES=False \
	PYTHONPATH=$(PWD) \
	airflow standalone

trigger:
	AIRFLOW_HOME=$(AIRFLOW_HOME) \
	PYTHONPATH=$(PWD) \
	airflow dags trigger weather_forecast

dashboard:
	PYTHONPATH=$(PWD) streamlit run dashboard/app.py

smoke:
	PYTHONPATH=$(PWD) $(PYTHON) -m pipeline.smoke

test:
	PYTHONPATH=$(PWD) pytest tests/ -v

clean:
	rm -rf data/raw/* data/weather.duckdb __pycache__ pipeline/__pycache__ dags/__pycache__ tests/__pycache__
