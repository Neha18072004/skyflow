"""Publish: rebuild the dashboard marts locally and copy them to a cloud Postgres (e.g. Neon).

Only the small marts.* tables leave the laptop - the 414 MB fact table stays local. The cloud copy
replaces every mart (TRUNCATE + COPY) inside ONE transaction, so the dashboard never sees a
half-published state. TRUNCATE rather than DELETE because it frees space immediately instead of
leaving dead rows - which matters on a 0.5 GB free tier. The cost: dashboard reads wait for the
few seconds the publish transaction holds its lock.

    CLOUD_DATABASE_URL=postgresql://... python -m skyflow.publish
Without CLOUD_DATABASE_URL it only rebuilds the marts locally.
"""

import io
import logging
import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from skyflow.db import get_engine, run_script, sql_statements

log = logging.getLogger(__name__)

MART_TABLES = [
    "marts.daily_ops", "marts.delay_heatmap", "marts.route_daily",
    "marts.dq_summary", "marts.pipeline_summary",
]


def cloud_url() -> str | None:
    url = os.getenv("CLOUD_DATABASE_URL", "").strip()
    if not url:
        return None
    # Neon hands out postgresql:// (or postgres://) URLs; SQLAlchemy 1.4 needs the driver named.
    for prefix in ("postgresql://", "postgres://"):
        if url.startswith(prefix):
            return "postgresql+psycopg2://" + url[len(prefix):]
    return url


def build_marts(engine: Engine) -> dict[str, int]:
    run_script(engine, "marts_schema.sql")
    with engine.begin() as conn:
        for stmt in sql_statements("marts.sql"):
            conn.execute(text(stmt))
    with engine.connect() as conn:
        counts = {t: conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar() for t in MART_TABLES}
    log.info("built marts locally: %s", counts)
    return counts


def copy_marts(source: Engine, target: Engine) -> dict[str, int]:
    """Copy every mart table source -> target with COPY, all in one target transaction."""
    run_script(target, "marts_schema.sql")
    counts = {}
    with source.connect() as src, target.begin() as dst:
        with src.connection.cursor() as src_cur, dst.connection.cursor() as dst_cur:
            for table in MART_TABLES:
                buf = io.StringIO()
                src_cur.copy_expert(f"COPY {table} TO STDOUT WITH CSV", buf)
                buf.seek(0)
                dst_cur.execute(f"TRUNCATE {table}")
                dst_cur.copy_expert(f"COPY {table} FROM STDIN WITH CSV", buf)
                dst_cur.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = dst_cur.fetchone()[0]
    log.info("published to cloud: %s", counts)
    return counts


def publish() -> dict[str, int] | None:
    """Build marts locally; copy them to the cloud if CLOUD_DATABASE_URL is set.
    Returns the cloud row counts, or None when there's no cloud target."""
    local_counts = build_marts(get_engine())
    url = cloud_url()
    if url is None:
        log.info("CLOUD_DATABASE_URL not set: marts built locally only")
        return None
    cloud = create_engine(url, pool_pre_ping=True)
    try:
        cloud_counts = copy_marts(get_engine(), cloud)
    finally:
        cloud.dispose()
    if cloud_counts != local_counts:
        raise RuntimeError(f"cloud row counts {cloud_counts} differ from local {local_counts}")
    return cloud_counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    publish()
