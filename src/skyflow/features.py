"""Build the ML feature table for one flight date by running sql/features.sql.

The logic lives in SQL because it is a set-based join/aggregate over warehouse tables, which
Postgres does in place - no need to pull history into pandas.
"""

import logging
from datetime import date, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine

from skyflow import config
from skyflow.load import date_key

log = logging.getLogger(__name__)


def feature_statements() -> list[str]:
    """features.sql holds a DELETE and an INSERT; run them separately in one transaction.
    Lines starting with -- are dropped first so a comment can't hide a statement boundary."""
    sql = (config.SQL_DIR / "features.sql").read_text()
    body = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    return [s.strip() for s in body.split(";") if s.strip()]


def build_features(flight_date: date, engine: Engine) -> int:
    params = {
        "flight_date": flight_date,
        "day_key": date_key(flight_date),
        "history_start_key": date_key(flight_date - timedelta(days=config.HISTORY_WINDOW_DAYS)),
    }
    delete_sql, insert_sql = feature_statements()
    with engine.begin() as conn:
        conn.execute(text(delete_sql), params)
        inserted = conn.execute(text(insert_sql), params).rowcount
    log.info("%s: built %s feature rows", flight_date, inserted)
    return inserted
