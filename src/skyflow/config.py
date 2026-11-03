"""Central configuration. Everything environment-specific comes from env vars so the same
code runs on a laptop, inside the Airflow containers and in CI."""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DB_URL = os.getenv(
    "SKYFLOW_DB_URL", "postgresql+psycopg2://skyflow:skyflow@localhost:5442/skyflow"
)
DATA_DIR = Path(os.getenv("SKYFLOW_DATA_DIR", PROJECT_ROOT / "data"))
RAW_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"
SQL_DIR = Path(os.getenv("SKYFLOW_SQL_DIR", PROJECT_ROOT / "sql"))

# Source columns we use, mapped to warehouse-friendly snake_case names.
# The BTS file has 110 columns; the rest are diversion details we don't model.
SOURCE_COLUMNS = {
    "FlightDate": "flight_date",
    "Reporting_Airline": "airline_code",
    "Tail_Number": "tail_number",
    "Flight_Number_Reporting_Airline": "flight_number",
    "Origin": "origin",
    "OriginCityName": "origin_city",
    "OriginState": "origin_state",
    "Dest": "dest",
    "DestCityName": "dest_city",
    "DestState": "dest_state",
    "CRSDepTime": "crs_dep_time",
    "DepTime": "dep_time",
    "DepDelay": "dep_delay_min",
    "CRSArrTime": "crs_arr_time",
    "ArrTime": "arr_time",
    "ArrDelay": "arr_delay_min",
    "Cancelled": "is_cancelled",
    "CancellationCode": "cancellation_code",
    "Diverted": "is_diverted",
    "CRSElapsedTime": "sched_elapsed_min",
    "ActualElapsedTime": "actual_elapsed_min",
    "Distance": "distance_miles",
    "CarrierDelay": "carrier_delay_min",
    "WeatherDelay": "weather_delay_min",
    "NASDelay": "nas_delay_min",
    "SecurityDelay": "security_delay_min",
    "LateAircraftDelay": "late_aircraft_delay_min",
}

# Columns that identify one flight. Used for duplicate detection and the warehouse unique key.
NATURAL_KEY = ["flight_date", "airline_code", "flight_number", "origin"]

# Delay bounds (minutes) for range checks. Early arrivals of more than 3 hours or delays of
# more than 2 days are almost certainly recording errors rather than real operations.
MIN_DELAY_MIN = -180
MAX_DELAY_MIN = 2880

# If more than this share of a day's rows fail row-level checks, something is wrong with the
# feed itself, so the run fails instead of quietly dropping a large chunk of data.
MAX_REJECT_RATE = 0.01

# The source only carries carrier codes. Names from the BTS L_UNIQUE_CARRIERS lookup.
AIRLINE_NAMES = {
    "9E": "Endeavor Air",
    "AA": "American Airlines",
    "AS": "Alaska Airlines",
    "B6": "JetBlue Airways",
    "DL": "Delta Air Lines",
    "F9": "Frontier Airlines",
    "G4": "Allegiant Air",
    "HA": "Hawaiian Airlines",
    "MQ": "Envoy Air",
    "NK": "Spirit Airlines",
    "OH": "PSA Airlines",
    "OO": "SkyWest Airlines",
    "UA": "United Airlines",
    "WN": "Southwest Airlines",
    "YX": "Republic Airline",
}

# Feature-table settings: trailing window used for historical delay rates.
HISTORY_WINDOW_DAYS = 28
