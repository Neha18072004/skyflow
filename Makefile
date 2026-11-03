.PHONY: setup data up down reset-db logs lint test run-day baseline publish dashboard

PY := .venv/bin/python
WAREHOUSE := docker compose exec -T warehouse psql -U skyflow -d skyflow

setup:            ## create the local Python 3.11 venv
	python3.11 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt

data:             ## download Jan-Mar 2024 BTS on-time data into data/raw/
	scripts/download_data.sh 2024 1 3

up:               ## start Postgres + Airflow (UI: http://localhost:8080, admin/admin)
	docker compose up -d

down:
	docker compose down

reset-db:         ## wipe warehouse tables and re-apply the schema (Airflow history is kept)
	$(WAREHOUSE) -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
	$(WAREHOUSE) < sql/schema.sql

logs:
	docker compose logs -f airflow-scheduler

lint:
	.venv/bin/ruff check .

test:             ## needs the warehouse running (make up) for the integration tests
	PYTHONPATH=src .venv/bin/pytest -v

run-day:          ## run one day outside Airflow: make run-day DATE=2024-01-01
	PYTHONPATH=src $(PY) -m skyflow.pipeline --start $(DATE)

baseline:         ## train the quick delay-prediction baseline on the feature table
	PYTHONPATH=src $(PY) scripts/train_baseline.py

publish:          ## rebuild dashboard marts; copy to cloud if CLOUD_DATABASE_URL is in .env
	set -a; [ -f .env ] && . ./.env; set +a; PYTHONPATH=src $(PY) -m skyflow.publish

dashboard:        ## run the Streamlit dashboard locally (reads DASHBOARD_DB_URL or the local warehouse)
	.venv/bin/streamlit run dashboard/app.py
