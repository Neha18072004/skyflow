import os
from datetime import date

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError

from skyflow.db import apply_schema

# Integration tests use their own database so they never touch real warehouse data.
TEST_DB_URL = os.getenv(
    "SKYFLOW_TEST_DB_URL", "postgresql+psycopg2://skyflow:skyflow@localhost:5442/skyflow_test"
)

DAY1 = date(2024, 1, 1)
DAY2 = date(2024, 1, 2)


def raw_row(**overrides) -> dict:
    """One valid raw row, all strings like the extract output. Override fields per test."""
    row = {
        "flight_date": "2024-01-01", "airline_code": "AA", "tail_number": "N101AA",
        "flight_number": "100", "origin": "JFK", "origin_city": "New York, NY",
        "origin_state": "NY", "dest": "LAX", "dest_city": "Los Angeles, CA", "dest_state": "CA",
        "crs_dep_time": "800", "dep_time": "805", "dep_delay_min": "5.00",
        "crs_arr_time": "1130", "arr_time": "1150", "arr_delay_min": "20.00",
        "is_cancelled": "0.00", "cancellation_code": None, "is_diverted": "0.00",
        "sched_elapsed_min": "390.00", "actual_elapsed_min": "405.00", "distance_miles": "2475.00",
        "carrier_delay_min": "20.00", "weather_delay_min": "0.00", "nas_delay_min": "0.00",
        "security_delay_min": "0.00", "late_aircraft_delay_min": "0.00",
    }
    row.update(overrides)
    return row


def raw_frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows, dtype=object)


@pytest.fixture
def valid_day() -> pd.DataFrame:
    """200 distinct valid flights: big enough that one bad row stays under the 1% reject cap."""
    return raw_frame([raw_row(flight_number=str(n)) for n in range(1, 201)])


@pytest.fixture(scope="session")
def engine():
    url = make_url(TEST_DB_URL)
    admin = create_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :d"), {"d": url.database}
            ).scalar()
            if not exists:
                conn.execute(text(f'CREATE DATABASE "{url.database}"'))
    except OperationalError:
        # Locally, skip so unit tests still run without Docker. In CI (GitHub sets CI=true) a
        # missing database must fail the build, not silently skip the integration tests.
        if os.getenv("CI"):
            raise
        pytest.skip("warehouse Postgres not reachable; start it with `docker compose up -d warehouse`")
    finally:
        admin.dispose()

    eng = create_engine(TEST_DB_URL)
    with eng.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE; CREATE SCHEMA public;"))
    apply_schema(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def clean_db(engine):
    """Empty the data tables between tests (dimension seeds dim_date/dim_time are kept)."""
    with engine.begin() as conn:
        conn.execute(text(
            "TRUNCATE features_flight_delay, fact_flights, dim_airline, dim_airport, dq_results"
        ))
    return engine
