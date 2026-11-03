"""Integration tests against a real Postgres (the `skyflow_test` database)."""

from sqlalchemy import text

from skyflow.features import build_features
from skyflow.load import load_dimensions, load_facts
from skyflow.transform import transform_frame

from .conftest import DAY1, DAY2, raw_frame, raw_row


def count(engine, table: str) -> int:
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()


def load_day(engine, rows, flight_date, batch_id="test"):
    df = transform_frame(raw_frame(rows))
    load_dimensions(df, engine)
    inserted = load_facts(df, flight_date, batch_id, engine)
    build_features(flight_date, engine)
    return inserted


def day_rows(day: str, n: int, late_every: int = 0) -> list[dict]:
    """n flights on `day`; every `late_every`-th one arrives 30 min late."""
    return [
        raw_row(flight_date=day, flight_number=str(i),
                arr_delay_min="30.00" if late_every and i % late_every == 0 else "0.00")
        for i in range(1, n + 1)
    ]


def test_running_the_same_partition_twice_gives_identical_counts(clean_db):
    rows = day_rows("2024-01-01", 50)
    first = load_day(clean_db, rows, DAY1)
    counts_after_first = {t: count(clean_db, t) for t in
                          ["fact_flights", "features_flight_delay", "dim_airline", "dim_airport"]}
    second = load_day(clean_db, rows, DAY1, batch_id="rerun")
    counts_after_second = {t: count(clean_db, t) for t in counts_after_first}

    assert first == second == 50
    assert counts_after_first == counts_after_second
    with clean_db.connect() as conn:
        assert conn.execute(text("SELECT DISTINCT batch_id FROM fact_flights")).scalar() == "rerun"


def test_rerun_replaces_the_partition_and_leaves_other_days_alone(clean_db):
    load_day(clean_db, day_rows("2024-01-01", 50), DAY1)
    load_day(clean_db, day_rows("2024-01-02", 40), DAY2)
    load_day(clean_db, day_rows("2024-01-01", 45), DAY1)   # source corrected: 5 flights removed

    with clean_db.connect() as conn:
        per_day = dict(conn.execute(text(
            "SELECT date_key, COUNT(*) FROM fact_flights GROUP BY date_key"
        )).fetchall())
    assert per_day == {20240101: 45, 20240102: 40}


def test_historical_rates_use_only_prior_days(clean_db):
    # Day 1: every 4th flight late -> 25% delay rate on the JFK-LAX route.
    load_day(clean_db, day_rows("2024-01-01", 40, late_every=4), DAY1)
    # Day 2: every flight late. If day 2 leaked into its own features the rate would rise.
    load_day(clean_db, day_rows("2024-01-02", 40, late_every=1), DAY2)

    with clean_db.connect() as conn:
        rows = conn.execute(text("""
            SELECT flight_date, MIN(route_delay_rate_28d), MAX(route_delay_rate_28d),
                   MIN(route_flights_28d), MIN(airline_delay_rate_28d)
            FROM features_flight_delay GROUP BY flight_date ORDER BY flight_date
        """)).fetchall()
    day1, day2 = rows
    assert day1[1] is None and day1[3] == 0          # no history before the first day
    assert day2[1] == day2[2] == 0.25                 # exactly day 1's rate, not day 2's
    assert day2[3] == 40 and day2[4] == 0.25
