# SkyFlow — Flight Operations Data Pipeline

[![CI](https://github.com/Neha18072004/skyflow/actions/workflows/ci.yml/badge.svg)](https://github.com/Neha18072004/skyflow/actions/workflows/ci.yml)

SkyFlow is an end-to-end batch ELT pipeline for airline operations data. It ingests US DOT/BTS
on-time performance records (1.66M flights, Jan–Mar 2024) and validates each day with 16
data-quality checks. It loads the data into a PostgreSQL star schema with idempotent per-day
delete-insert, and builds a leakage-safe feature table for flight-delay prediction. Apache Airflow
orchestrates the pipeline with daily partitions, retries and backfills. Everything runs locally
with `docker compose up` (native on Apple Silicon).

## Architecture

```mermaid
flowchart LR
    A[BTS monthly CSVs<br/>data/raw] -->|extract| B[staging/flight_date=D<br/>raw.parquet]
    B -->|validate| C{16 DQ checks}
    C -->|critical fail| X[DAG run fails]
    C -->|bad rows| R[rejected.parquet]
    C -->|good rows| D[validated.parquet]
    D -->|transform| E[clean.parquet]
    E -->|load_dimensions| F[(dim_airline<br/>dim_airport)]
    E -->|load_facts: COPY + delete-insert| G[(fact_flights)]
    G -->|build_features: SQL| H[(features_flight_delay)]
    G & H -->|dq_report: reconcile| Q[(dq_results)]
    C --> Q
    G & Q -->|publish_marts: aggregate| M[(marts.* 33 MB)]
    M -->|COPY, one transaction| N[(Neon Postgres)]
    N --> S[Streamlit dashboard]
    subgraph Airflow LocalExecutor - one DAG run per flight date
      B
      C
      D
      E
    end
```

Two Postgres 16 instances: **warehouse** (`localhost:5442`) and **Airflow metadata** (internal).
Only the small pre-aggregated marts go to the cloud. The 414 MB fact table stays local, which
keeps the cloud copy well inside Neon's free tier.

## Live dashboard

**URL:** TODO

![SkyFlow dashboard, Overview tab](docs/dashboard.png)

A Streamlit dashboard reading only the `marts.*` tables in Neon. It has four tabs: Overview
(KPIs vs the previous period, daily on-time trend, delay causes), Airlines & Routes (league
table, most-delayed routes), Delay Patterns (hour × weekday heatmap) and Pipeline Health (run
summary and the 16 DQ checks). Filters are date range, airline and origin airport.

| Mart | Rows | Size | Grain |
|---|---|---|---|
| `marts.daily_ops` | 127,213 | 16 MB | date × airline × origin |
| `marts.delay_heatmap` | 81,230 | 7.4 MB | airline × origin × weekday × hour |
| `marts.route_daily` | 91,803 | 9 MB | date × airline × route (300 busiest routes) |
| `marts.dq_summary` | 1,456 | 0.4 MB | date × check (latest result) |
| `marts.pipeline_summary` | 1 | — | headline run numbers |

```bash
make publish      # rebuild marts locally; copy to Neon if CLOUD_DATABASE_URL is in .env
make dashboard    # run the dashboard locally (falls back to the local warehouse)
```

## Quickstart

```bash
git clone <repo> && cd skyflow
scripts/download_data.sh 2024 1 3      # ~750 MB of CSV into data/raw/
docker compose up -d                    # warehouse + Airflow; DAG starts catching up immediately
open http://localhost:8080              # admin / admin
```

With `SKYFLOW_CATCHUP=true` (the default), Airflow backfills every day from `SKYFLOW_START_DATE`
to `SKYFLOW_END_DATE`. To re-run or backfill a range manually:

```bash
docker compose exec airflow-scheduler airflow tasks clear skyflow_pipeline -s 2024-02-14 -e 2024-02-16 -y
```

Local development (Python 3.11):

```bash
python3.11 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/ruff check . && .venv/bin/pytest -v          # integration tests use localhost:5442
PYTHONPATH=src .venv/bin/python -m skyflow.pipeline --start 2024-01-01   # one day, no Airflow
PYTHONPATH=src .venv/bin/python scripts/train_baseline.py               # baseline model
```

## DAG

`extract → validate → transform → load_dimensions → load_facts → build_features → dq_report → publish_marts`

![DAG graph view](docs/dag.png)

![91-day backfill, all runs successful](docs/dag_runs.png)
*91-day backfill, all runs successful*

`publish_marts` was added after the backfill. It is skipped when `CLOUD_DATABASE_URL` isn't
set, so CI and other people's local runs still work.

- Daily schedule. Each run processes the flights of its logical date, so re-runs and backfills
  are the same operation.
- `retries=2` (1 min apart), 10-min task timeout, 1-h run timeout.
- `max_active_runs=4` for parallel loading. `build_features` uses `depends_on_past` because its
  rolling history needs every earlier day loaded first.
- The DAG file only orchestrates. All logic lives in `src/skyflow/` and runs the same way from
  the CLI and in tests.

## Warehouse schema

Grain of `fact_flights`: **one row per scheduled flight leg**, keyed by
(flight date, airline, flight number, origin).

```mermaid
erDiagram
    dim_date    ||--o{ fact_flights : "date_key"
    dim_airline ||--o{ fact_flights : "airline_key"
    dim_airport ||--o{ fact_flights : "origin_airport_key"
    dim_airport ||--o{ fact_flights : "dest_airport_key"
    dim_time    ||--o{ fact_flights : "sched_dep/arr_time_key"
    fact_flights ||--o| features_flight_delay : "flight_key"

    fact_flights {
        bigint flight_key PK
        int date_key FK
        smallint airline_key FK
        int origin_airport_key FK
        int dest_airport_key FK
        smallint sched_dep_time_key FK
        int flight_number
        timestamp sched_dep_at
        timestamp actual_dep_at
        smallint dep_delay_min
        smallint arr_delay_min
        bool is_arr_delayed_15
        bool is_cancelled
        bool is_diverted
        smallint distance_miles
    }
    dim_date {
        int date_key PK
        date full_date
        smallint day_of_week
        bool is_weekend
    }
    dim_time {
        smallint time_key PK
        smallint hour
        char time_block
        varchar day_part
    }
    dim_airline {
        smallint airline_key PK
        varchar airline_code UK
        varchar airline_name
    }
    dim_airport {
        int airport_key PK
        char airport_code UK
        varchar city_name
        char state_code
    }
    features_flight_delay {
        bigint flight_key PK
        smallint dep_hour
        real route_delay_rate_28d
        real airline_delay_rate_28d
        bool is_arr_delayed_15
    }
```

## Data quality

16 checks, every result stored in `dq_results` (run id, date, stage, severity, failed rows):

- **Critical. The run fails and nothing bad is loaded:** required columns, non-empty partition,
  nulls in key fields, duplicate natural keys, a reject rate above 1%. After the load, three
  reconciliation checks: raw = clean + rejected, staged = warehouse, and features = flown flights.
- **Row-level warnings. Bad rows go to `rejected.parquet` with a reason:** numeric types, HHMM
  format, distance > 0, scheduled block time > 0, delay within −180…2,880 min, airport code
  format, origin ≠ destination, cancel/divert flags consistent with times.

The 1% reject cap means a broken feed fails loudly instead of being silently filtered away.
During development it caught a real extract bug that turned empty cells into `"nan"`.
See [docs/DESIGN.md](docs/DESIGN.md) for the full rationale.

## Results

Every figure below comes from the run on 2026-11-03: 91 daily DAG runs on a MacBook Air M4,
Docker Desktop with 7.6 GB RAM. The source is `sql/run_metrics.sql` plus Airflow's metadata DB.

| Metric | Value |
|---|---|
| Raw records ingested | **1,658,259** (Jan 1 – Mar 31 2024, 15 airlines, 334 airports) |
| Records loaded after validation | **1,658,251** |
| Rows rejected | **8**: 6 delays beyond 48 h, 2 missing or negative scheduled block time |
| Critical check failures | 0 |
| DQ checks | 16 distinct checks × 91 partitions = **1,456** results recorded |
| DAG runs | **91 / 91 succeeded**, 0 task retries |
| Pipeline runtime (full 91-day backfill) | **5 min 3 s** wall clock (4 concurrent runs) |
| Mean task time per day | extract 0.92 s, validate 0.43 s, transform 0.36 s, load_dims 0.30 s, load_facts 0.88 s, features 0.56 s, dq_report 0.26 s |
| `fact_flights` | 1,658,251 rows (414 MB with indexes) |
| `features_flight_delay` | **1,626,046** rows (187 MB): all flights not cancelled (28,509) or diverted (3,696) |
| Dimensions | 15 airlines, 334 airports, 4,018 dates, 1,440 times |
| Arrival delay ≥ 15 min rate | 20.32% of flown flights |
| Idempotency | Cleared and re-ran Feb 14–16 in Airflow: per-day counts unchanged (16,762 / 19,013 / 19,163), neighbouring days untouched |
| Baseline model | HistGradientBoosting, time-split (train Jan 29–Mar 10: 749,912 rows, test Mar 11–31: 401,905 rows): **ROC AUC 0.663** vs 0.615 for the route delay rate alone |

The AUC is modest by design: features are limited to what is known before departure, with no
weather or inbound-aircraft delay data.

## Project layout

```
dags/skyflow_pipeline.py     Airflow DAG (orchestration only)
src/skyflow/                 extract, validate, transform, load, features, pipeline, config, db
sql/schema.sql               star schema + dq_results + feature table DDL
sql/features.sql             per-day feature build (leakage-safe windows)
sql/run_metrics.sql          numbers for the Results table
sql/marts_schema.sql, marts.sql   dashboard marts (DDL + full refresh)
src/skyflow/publish.py       build marts, copy them to the cloud Postgres
dashboard/app.py             Streamlit dashboard (reads marts only)
.github/workflows/           ci.yml (lint + tests), publish.yml (manual full rebuild + publish)
scripts/                     data download, baseline model
tests/                       unit (validate, transform) + integration (idempotency, leakage)
docs/DESIGN.md               design decisions and trade-offs
```

## Data source and scope

**US BTS On-Time Performance, Jan–Mar 2024** (Bureau of Transportation Statistics, *Reporting
Carrier On-Time Performance*, public domain). The pipeline doesn't depend on this source and
could run on any airline's operational data: adapting it means changing the column map in
`config.py` and the extract step.
