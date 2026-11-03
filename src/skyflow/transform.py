"""Transform: turn validated string data into typed, warehouse-shaped rows.

Main jobs: cast types, convert BTS HHMM integers into real timestamps, derive the delay label,
and map each scheduled time to its dim_time key.
"""

import logging
from datetime import date

import numpy as np
import pandas as pd

from skyflow import config
from skyflow.extract import partition_dir

log = logging.getLogger(__name__)

INT_COLUMNS = [
    "flight_number", "dep_delay_min", "arr_delay_min", "sched_elapsed_min", "actual_elapsed_min",
    "distance_miles", "carrier_delay_min", "weather_delay_min", "nas_delay_min",
    "security_delay_min", "late_aircraft_delay_min",
]


def hhmm_to_minutes(hhmm: pd.Series) -> pd.Series:
    """1435 -> 875 minutes after midnight. BTS uses 2400 for midnight at the end of the day,
    which becomes 1440 (= next day 00:00) when added to the flight date."""
    return (hhmm // 100) * 60 + hhmm % 100


def hhmm_to_time_key(hhmm: pd.Series) -> pd.Series:
    """dim_time keys run 0000-2359, so 2400 folds back to 0000."""
    return (hhmm % 2400).astype("int16")


def arrival_day_offset(dep_min: pd.Series, arr_min: pd.Series, elapsed_min: pd.Series) -> pd.Series:
    """Which calendar day (relative to departure) the scheduled arrival falls on: -1, 0 or +1.

    Departure and arrival are local times in possibly different time zones, and the source has no
    zone info. So we pick the day offset that makes (arrival - departure) closest to the scheduled
    block time. The leftover error is just the time-zone difference (at most ~6h within the US),
    far less than the 24h that separates the candidate days, so the choice is unambiguous.
    A naive "arrival < departure means next day" rule gets short westbound hops wrong, e.g.
    07:00 ET -> 06:55 CT is the same day.
    """
    gap = (arr_min - dep_min).to_numpy(dtype=float)
    offsets = np.array([-1, 0, 1])
    errors = np.abs(gap[:, None] + offsets[None, :] * 1440 - elapsed_min.to_numpy(dtype=float)[:, None])
    return pd.Series(offsets[errors.argmin(axis=1)], index=dep_min.index)


def transform_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Pure function: validated raw rows in, typed fact-ready rows out."""
    out = pd.DataFrame(index=df.index)
    out["flight_date"] = pd.to_datetime(df["flight_date"]).dt.date
    out["airline_code"] = df["airline_code"].str.strip()
    out["airline_name"] = out["airline_code"].map(config.AIRLINE_NAMES).fillna(out["airline_code"])
    out["origin"] = df["origin"]
    out["origin_city"] = df["origin_city"]
    out["origin_state"] = df["origin_state"]
    out["dest"] = df["dest"]
    out["dest_city"] = df["dest_city"]
    out["dest_state"] = df["dest_state"]
    out["tail_number"] = df["tail_number"]

    for col in INT_COLUMNS:
        # Float first because source values look like "15.00"; nullable Int64 keeps NULLs.
        out[col] = pd.to_numeric(df[col]).round().astype("Int64")
    out["is_cancelled"] = pd.to_numeric(df["is_cancelled"]).astype(bool)
    out["is_diverted"] = pd.to_numeric(df["is_diverted"]).astype(bool)
    out["cancellation_code"] = df["cancellation_code"]

    crs_dep = pd.to_numeric(df["crs_dep_time"]).astype(int)
    crs_arr = pd.to_numeric(df["crs_arr_time"]).astype(int)
    out["sched_dep_time_key"] = hhmm_to_time_key(crs_dep)
    out["sched_arr_time_key"] = hhmm_to_time_key(crs_arr)

    day_start = pd.to_datetime(df["flight_date"])
    dep_min = hhmm_to_minutes(crs_dep)
    arr_min = hhmm_to_minutes(crs_arr)
    arr_offset = arrival_day_offset(dep_min, arr_min, out["sched_elapsed_min"])

    out["sched_dep_at"] = day_start + pd.to_timedelta(dep_min, unit="min")
    out["sched_arr_at"] = day_start + pd.to_timedelta(arr_min + arr_offset * 1440, unit="min")
    # Actual = scheduled + delay. Using the delay instead of the raw DepTime/ArrTime columns
    # sidesteps midnight roll-over entirely (a 23:50 flight 30 min late departs the next day).
    def minutes(col: str) -> pd.Series:
        return pd.to_timedelta(out[col].astype(float), unit="min")

    out["actual_dep_at"] = out["sched_dep_at"] + minutes("dep_delay_min")
    out["actual_arr_at"] = out["sched_arr_at"] + minutes("arr_delay_min")

    # Standard DOT on-time definition: arriving 15+ minutes late. Undefined if it never arrived.
    flown = ~out["is_cancelled"] & ~out["is_diverted"]
    out["is_arr_delayed_15"] = (out["arr_delay_min"] >= 15).astype("boolean").where(flown, pd.NA)
    return out.reset_index(drop=True)


def transform(flight_date: date) -> int:
    part = partition_dir(flight_date)
    clean = transform_frame(pd.read_parquet(part / "validated.parquet"))
    clean.to_parquet(part / "clean.parquet", index=False)
    log.info("transformed %s rows for %s", len(clean), flight_date)
    return len(clean)
