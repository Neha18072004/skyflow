"""One function per pipeline step, keyed by flight date. The Airflow DAG calls these, and the
same steps can be run by hand from the command line:

    python -m skyflow.pipeline --start 2024-01-01 --end 2024-01-07

Steps exchange data through files in data/staging/flight_date=YYYY-MM-DD/, not XCom: each task
can be retried or inspected on its own, and XCom is meant for small metadata, not datasets.
"""

import argparse
import logging
import time
from datetime import date, timedelta

from skyflow import extract as extract_mod
from skyflow import features, load
from skyflow import transform as transform_mod
from skyflow import validate as validate_mod
from skyflow.db import get_engine

log = logging.getLogger(__name__)


def run_extract(flight_date: date) -> int:
    return extract_mod.extract(flight_date)


def run_validate(flight_date: date, run_id: str) -> int:
    return validate_mod.validate(flight_date, run_id, get_engine()).rejected_rows


def run_transform(flight_date: date) -> int:
    return transform_mod.transform(flight_date)


def run_load_dimensions(flight_date: date) -> dict[str, int]:
    return load.load_dimensions(load.read_clean(flight_date), get_engine())


def run_load_facts(flight_date: date, run_id: str) -> int:
    return load.load_facts(load.read_clean(flight_date), flight_date, run_id, get_engine())


def run_build_features(flight_date: date) -> int:
    return features.build_features(flight_date, get_engine())


def run_dq_report(flight_date: date, run_id: str) -> None:
    validate_mod.reconcile(flight_date, run_id, get_engine())


def run_day(flight_date: date, run_id: str = "manual") -> None:
    run_extract(flight_date)
    run_validate(flight_date, run_id)
    run_transform(flight_date)
    run_load_dimensions(flight_date)
    run_load_facts(flight_date, run_id)
    run_build_features(flight_date)
    run_dq_report(flight_date, run_id)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the SkyFlow pipeline outside Airflow.")
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, help="inclusive; defaults to --start")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    day, end = args.start, args.end or args.start
    started = time.perf_counter()
    # Days run in order: features for day D read history from days before D.
    while day <= end:
        run_day(day)
        day += timedelta(days=1)
    log.info("done in %.1fs", time.perf_counter() - started)


if __name__ == "__main__":
    main()
