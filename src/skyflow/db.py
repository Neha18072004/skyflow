"""Small database helpers shared by load, features and the DQ report."""

from functools import cache

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from skyflow import config


@cache
def get_engine(url: str | None = None) -> Engine:
    # pool_pre_ping avoids errors from connections the DB closed while a task was idle.
    return create_engine(url or config.DB_URL, pool_pre_ping=True)


def apply_schema(engine: Engine | None = None) -> None:
    """Apply sql/schema.sql. Safe to run repeatedly: every statement is idempotent."""
    engine = engine or get_engine()
    ddl = (config.SQL_DIR / "schema.sql").read_text()
    # Raw DBAPI cursor with no parameters: psycopg2 then sends the multi-statement script as-is.
    # (Through SQLAlchemy it gets an empty params dict and tries to interpolate the `%` modulo
    # operators in the dim_time generator.)
    with engine.begin() as conn:
        with conn.connection.cursor() as cur:
            cur.execute(ddl)


def run_script(engine: Engine, filename: str) -> None:
    """Execute a multi-statement SQL file from sql/ with no parameters (idempotent DDL)."""
    ddl = (config.SQL_DIR / filename).read_text()
    with engine.begin() as conn:
        with conn.connection.cursor() as cur:
            cur.execute(ddl)


def sql_statements(filename: str) -> list[str]:
    """Split a SQL file into statements for parameterised execution. Comment lines are dropped
    first so a comment can't hide a statement boundary."""
    sql = (config.SQL_DIR / filename).read_text()
    body = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    return [s.strip() for s in body.split(";") if s.strip()]


def scalar(sql: str, engine: Engine | None = None, **params):
    engine = engine or get_engine()
    with engine.connect() as conn:
        return conn.execute(text(sql), params).scalar()
