"""SkyFlow daily pipeline. This file only orchestrates: all business logic lives in src/skyflow.

Each DAG run processes the flights of its logical date (`ds`), so a run is one partition:
re-running it replaces that day, and a backfill is just a range of runs.
"""

import os
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task

START_DATE = datetime.fromisoformat(os.getenv("SKYFLOW_START_DATE", "2024-01-01"))
END_DATE_ENV = os.getenv("SKYFLOW_END_DATE")
END_DATE = datetime.fromisoformat(END_DATE_ENV) if END_DATE_ENV else None
CATCHUP = os.getenv("SKYFLOW_CATCHUP", "false").lower() == "true"

DOC_MD = """
### SkyFlow · Flight Operations Pipeline
Daily ELT of US DOT/BTS on-time data into a Postgres star schema plus an ML feature table.

**Flow:** Extract raw BTS data → Validate data quality → Transform & type-cast →
Load dimensions → Load facts → Build ML features → DQ reconciliation report

| | |
|---|---|
| **Source** | BTS *Reporting Carrier On-Time Performance*, monthly CSVs in `data/raw/` |
| **Schedule** | `@daily`. Each run processes the flights of its logical date |
| **Idempotency** | Per-day delete-insert in one transaction, so re-runs and backfills replace the day |
| **DQ policy** | Critical checks fail the run. Bad rows are quarantined with a reason. \
Over 1% rejects fails the day. Results go to `dq_results` |
"""

# Graph-view colours by pipeline stage: (background, text).
STAGE_COLORS = {
    "ingestion": ("#1E3A5F", "#FFFFFF"),       # navy
    "transformation": ("#475569", "#FFFFFF"),  # slate
    "loading": ("#0F766E", "#FFFFFF"),         # teal
    "ml": ("#5B4B8A", "#FFFFFF"),              # violet
    "reporting": ("#B45309", "#FFFFFF"),       # amber
}


def styled(task_output, stage: str):
    """Colour a TaskFlow task by stage. ui_color/ui_fgcolor aren't decorator arguments in
    Airflow 2.x, so they're set on the underlying operator instance."""
    task_output.operator.ui_color, task_output.operator.ui_fgcolor = STAGE_COLORS[stage]
    return task_output


default_args = {
    "owner": "skyflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    # A day normally takes seconds; anything past this is a hang (e.g. a lock), not slowness.
    "execution_timeout": timedelta(minutes=10),
}


@dag(
    dag_id="skyflow_pipeline",
    description="Daily ELT of BTS flight operations into the SkyFlow warehouse",
    schedule="@daily",
    start_date=START_DATE,
    # Source data only exists for a fixed historical range; stop scheduling past it.
    end_date=END_DATE,
    catchup=CATCHUP,
    # Days can extract/validate/load in parallel; build_features is serialized below.
    max_active_runs=4,
    dagrun_timeout=timedelta(hours=1),
    default_args=default_args,
    tags=["skyflow", "elt"],
    doc_md=DOC_MD,
)
def skyflow_pipeline():
    # Imports happen inside tasks so the scheduler's frequent DAG-file parsing stays cheap
    # (pandas/pyarrow are only loaded by the worker process that runs a task).

    @task(
        task_display_name="Extract raw BTS data",
        doc_md="Reads the logical date's flights from the monthly CSV into a staging Parquet partition.",
    )
    def extract(ds: str = None) -> int:
        from skyflow.pipeline import run_extract
        return run_extract(date.fromisoformat(ds))

    @task(
        task_display_name="Validate data quality",
        doc_md="Runs the 13 pre-load DQ checks, quarantines bad rows and fails the run on critical issues.",
    )
    def validate(ds: str = None, run_id: str = None) -> int:
        from skyflow.pipeline import run_validate
        return run_validate(date.fromisoformat(ds), run_id)

    @task(
        task_display_name="Transform & type-cast",
        doc_md="Casts types, builds local timestamps and derives the 15-minute delay label.",
    )
    def transform(ds: str = None) -> int:
        from skyflow.pipeline import run_transform
        return run_transform(date.fromisoformat(ds))

    @task(
        task_display_name="Load dimensions",
        doc_md="Upserts airlines and airports (SCD type 1).",
    )
    def load_dimensions(ds: str = None) -> dict:
        from skyflow.pipeline import run_load_dimensions
        return run_load_dimensions(date.fromisoformat(ds))

    @task(
        task_display_name="Load facts",
        doc_md="Replaces the day's rows in fact_flights via COPY and delete-insert in one transaction.",
    )
    def load_facts(ds: str = None, run_id: str = None) -> int:
        from skyflow.pipeline import run_load_facts
        return run_load_facts(date.fromisoformat(ds), run_id)

    # depends_on_past: day D's features read facts from days before D. Requiring the previous
    # run's build_features to have succeeded guarantees (by induction) that all earlier days
    # are loaded, while the other tasks still run in parallel across days during a backfill.
    @task(
        depends_on_past=True, task_display_name="Build ML features",
        doc_md="Rebuilds the day's delay features from the 28 days strictly before it, so no leakage.",
    )
    def build_features(ds: str = None) -> int:
        from skyflow.pipeline import run_build_features
        return run_build_features(date.fromisoformat(ds))

    @task(
        task_display_name="DQ reconciliation report",
        doc_md="Checks raw = clean + rejected, staged = warehouse, and features = flown flights.",
    )
    def dq_report(ds: str = None, run_id: str = None) -> None:
        from skyflow.pipeline import run_dq_report
        run_dq_report(date.fromisoformat(ds), run_id)

    (
        styled(extract(), "ingestion")
        >> styled(validate(), "ingestion")
        >> styled(transform(), "transformation")
        >> styled(load_dimensions(), "loading")
        >> styled(load_facts(), "loading")
        >> styled(build_features(), "ml")
        >> styled(dq_report(), "reporting")
    )


skyflow_pipeline()
