"""Build the ML feature table for one flight date by running sql/features.sql.

The logic lives in SQL because it is a set-based join/aggregate over warehouse tables, which
Postgres does in place - no need to pull history into pandas.
"""

import logging
from datetime import date, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine

from skyflow import config
from skyflow.db import sql_statements
from skyflow.load import date_key

log = logging.getLogger(__name__)


def build_features(flight_date: date, engine: Engine) -> int:
    params = {
        "flight_date": flight_date,
        "day_key": date_key(flight_date),
        "history_start_key": date_key(flight_date - timedelta(days=config.HISTORY_WINDOW_DAYS)),
    }
    # features.sql holds a DELETE and an INSERT; run them separately in one transaction.
    delete_sql, insert_sql = sql_statements("features.sql")
    with engine.begin() as conn:
        conn.execute(text(delete_sql), params)
        inserted = conn.execute(text(insert_sql), params).rowcount
    log.info("%s: built %s feature rows", flight_date, inserted)
    return inserted
