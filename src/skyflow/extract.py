"""Extract: pull one flight-date partition out of the raw monthly BTS files into staging.

Raw files are monthly (data/raw/ontime_YYYY_MM.csv). The pipeline works per day, so extract
reads the month that contains the partition date and writes just that day to
data/staging/flight_date=YYYY-MM-DD/raw.parquet. Everything is read as strings: type checking
belongs to validate, so a malformed value is reported rather than crashing the read.
"""

import logging
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
from pyarrow import csv as pa_csv

from skyflow import config

log = logging.getLogger(__name__)


def raw_file_for(flight_date: date, raw_dir: Path | None = None) -> Path:
    raw_dir = raw_dir or config.RAW_DIR
    return raw_dir / f"ontime_{flight_date:%Y_%m}.csv"


def partition_dir(flight_date: date, staging_dir: Path | None = None) -> Path:
    staging_dir = staging_dir or config.STAGING_DIR
    return staging_dir / f"flight_date={flight_date:%Y-%m-%d}"


def read_raw_partition(flight_date: date, raw_dir: Path | None = None) -> pd.DataFrame:
    path = raw_file_for(flight_date, raw_dir)
    if not path.exists():
        raise FileNotFoundError(f"No raw file for {flight_date}: expected {path}")

    # Only read the columns we need (27 of 110), and only those present: a missing required
    # column should surface as a schema-check failure in validate, not a read error here.
    header = pd.read_csv(path, nrows=0).columns
    usecols = [c for c in config.SOURCE_COLUMNS if c in header]
    # pyarrow's CSV reader is multi-threaded and several times faster than pandas'. We call it
    # directly because pd.read_csv(engine="pyarrow", dtype=str) parses numbers first and then
    # casts, turning empty cells into the string "nan" instead of a null.
    table = pa_csv.read_csv(
        path,
        convert_options=pa_csv.ConvertOptions(
            include_columns=usecols,
            column_types={c: pa.string() for c in usecols},
            strings_can_be_null=True,  # empty field -> null, not ""
        ),
    )
    df = table.to_pandas().rename(columns=config.SOURCE_COLUMNS)
    return df[df["flight_date"] == flight_date.isoformat()].reset_index(drop=True)


def extract(flight_date: date, raw_dir: Path | None = None, staging_dir: Path | None = None) -> int:
    """Write the day's raw rows to staging, overwriting any earlier attempt. Returns row count."""
    df = read_raw_partition(flight_date, raw_dir)
    out_dir = partition_dir(flight_date, staging_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir / "raw.parquet", index=False)
    log.info("extracted %s rows for %s -> %s", len(df), flight_date, out_dir)
    return len(df)
