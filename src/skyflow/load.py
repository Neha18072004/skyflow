"""Load: upsert dimensions, then replace one day's facts in the warehouse.

Idempotency strategy for facts is delete-insert per partition inside ONE transaction:
    COPY day's rows -> temp table; DELETE that date from fact_flights; INSERT from temp table.
Re-running a day therefore replaces it exactly, and readers never see a half-loaded day
(Postgres MVCC shows them the old rows until COMMIT). We chose this over row-level
INSERT ... ON CONFLICT UPDATE because it also removes rows that disappeared from the source,
and it is simpler to reason about for a whole-day batch.
"""

import io
import logging
from datetime import date

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from skyflow.extract import partition_dir

log = logging.getLogger(__name__)

# Order of columns COPY'd into the temp staging table (must match STAGE_DDL).
STAGE_COLUMNS = [
    "flight_date", "airline_code", "flight_number", "tail_number", "origin", "dest",
    "sched_dep_time_key", "sched_arr_time_key", "sched_dep_at", "actual_dep_at",
    "sched_arr_at", "actual_arr_at", "dep_delay_min", "arr_delay_min", "is_arr_delayed_15",
    "is_cancelled", "cancellation_code", "is_diverted", "distance_miles", "sched_elapsed_min",
    "actual_elapsed_min", "carrier_delay_min", "weather_delay_min", "nas_delay_min",
    "security_delay_min", "late_aircraft_delay_min",
]

# ON COMMIT DROP: the temp table vanishes with the transaction, so retries start clean.
STAGE_DDL = """
CREATE TEMP TABLE stg_flights (
    flight_date DATE, airline_code VARCHAR(3), flight_number INTEGER, tail_number VARCHAR(10),
    origin CHAR(3), dest CHAR(3), sched_dep_time_key SMALLINT, sched_arr_time_key SMALLINT,
    sched_dep_at TIMESTAMP, actual_dep_at TIMESTAMP, sched_arr_at TIMESTAMP,
    actual_arr_at TIMESTAMP, dep_delay_min SMALLINT, arr_delay_min SMALLINT,
    is_arr_delayed_15 BOOLEAN, is_cancelled BOOLEAN, cancellation_code CHAR(1),
    is_diverted BOOLEAN, distance_miles SMALLINT, sched_elapsed_min SMALLINT,
    actual_elapsed_min SMALLINT, carrier_delay_min SMALLINT, weather_delay_min SMALLINT,
    nas_delay_min SMALLINT, security_delay_min SMALLINT, late_aircraft_delay_min SMALLINT
) ON COMMIT DROP
"""

INSERT_FACTS = """
INSERT INTO fact_flights (
    date_key, airline_key, origin_airport_key, dest_airport_key, sched_dep_time_key,
    sched_arr_time_key, flight_number, tail_number, sched_dep_at, actual_dep_at, sched_arr_at,
    actual_arr_at, dep_delay_min, arr_delay_min, is_arr_delayed_15, is_cancelled,
    cancellation_code, is_diverted, distance_miles, sched_elapsed_min, actual_elapsed_min,
    carrier_delay_min, weather_delay_min, nas_delay_min, security_delay_min,
    late_aircraft_delay_min, batch_id
)
SELECT
    d.date_key, al.airline_key, o.airport_key, de.airport_key, s.sched_dep_time_key,
    s.sched_arr_time_key, s.flight_number, s.tail_number, s.sched_dep_at, s.actual_dep_at,
    s.sched_arr_at, s.actual_arr_at, s.dep_delay_min, s.arr_delay_min, s.is_arr_delayed_15,
    s.is_cancelled, s.cancellation_code, s.is_diverted, s.distance_miles, s.sched_elapsed_min,
    s.actual_elapsed_min, s.carrier_delay_min, s.weather_delay_min, s.nas_delay_min,
    s.security_delay_min, s.late_aircraft_delay_min, :batch_id
FROM stg_flights s
-- Inner joins: load_dimensions runs first, so every code has a key. If one were missing the
-- row would drop here, and the post-load row-count reconciliation would catch it.
JOIN dim_date    d  ON d.full_date     = s.flight_date
JOIN dim_airline al ON al.airline_code = s.airline_code
JOIN dim_airport o  ON o.airport_code  = s.origin
JOIN dim_airport de ON de.airport_code = s.dest
"""


def date_key(flight_date: date) -> int:
    return int(flight_date.strftime("%Y%m%d"))


def read_clean(flight_date: date) -> pd.DataFrame:
    return pd.read_parquet(partition_dir(flight_date) / "clean.parquet")


def load_dimensions(df: pd.DataFrame, engine: Engine) -> dict[str, int]:
    """Upsert airlines and airports seen in this partition (SCD type 1: overwrite attributes).
    The WHERE clause skips no-op updates so unchanged rows aren't rewritten every day."""
    airlines = df[["airline_code", "airline_name"]].drop_duplicates("airline_code")
    airports = pd.concat([
        df[["origin", "origin_city", "origin_state"]].set_axis(["code", "city", "state"], axis=1),
        df[["dest", "dest_city", "dest_state"]].set_axis(["code", "city", "state"], axis=1),
    ]).drop_duplicates("code")

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO dim_airline (airline_code, airline_name) VALUES (:code, :name)
                ON CONFLICT (airline_code) DO UPDATE
                    SET airline_name = EXCLUDED.airline_name, updated_at = NOW()
                    WHERE dim_airline.airline_name IS DISTINCT FROM EXCLUDED.airline_name
            """),
            [{"code": r.airline_code, "name": r.airline_name} for r in airlines.itertuples()],
        )
        conn.execute(
            text("""
                INSERT INTO dim_airport (airport_code, city_name, state_code)
                VALUES (:code, :city, :state)
                ON CONFLICT (airport_code) DO UPDATE
                    SET city_name = EXCLUDED.city_name, state_code = EXCLUDED.state_code,
                        updated_at = NOW()
                    WHERE (dim_airport.city_name, dim_airport.state_code)
                          IS DISTINCT FROM (EXCLUDED.city_name, EXCLUDED.state_code)
            """),
            [{"code": r.code, "city": r.city, "state": r.state} for r in airports.itertuples()],
        )
    log.info("upserted %s airlines, %s airports", len(airlines), len(airports))
    return {"airlines": len(airlines), "airports": len(airports)}


def load_facts(df: pd.DataFrame, flight_date: date, batch_id: str, engine: Engine) -> int:
    """Replace the warehouse rows for one flight date. Returns rows inserted."""
    if (df["flight_date"] != flight_date).any():
        raise ValueError(f"partition for {flight_date} contains rows from other dates")

    buf = io.StringIO()
    # COPY's CSV format treats an unquoted empty field as NULL, which is what pandas writes for NA.
    df[STAGE_COLUMNS].to_csv(buf, index=False, header=False)
    buf.seek(0)

    with engine.begin() as conn:
        conn.execute(text(STAGE_DDL))
        # COPY is Postgres' bulk-load path: ~10x faster than batched INSERTs for 18k rows/day.
        with conn.connection.cursor() as cur:
            cur.copy_expert(f"COPY stg_flights ({', '.join(STAGE_COLUMNS)}) FROM STDIN WITH CSV", buf)
        deleted = conn.execute(
            text("DELETE FROM fact_flights WHERE date_key = :dk"), {"dk": date_key(flight_date)}
        ).rowcount
        inserted = conn.execute(text(INSERT_FACTS), {"batch_id": batch_id}).rowcount
    log.info("%s: deleted %s, inserted %s fact rows", flight_date, deleted, inserted)
    return inserted
