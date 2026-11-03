"""Validate: data-quality checks on one staged partition, before anything touches the warehouse.

Two kinds of checks:
  * critical  - the partition as a whole is untrustworthy (missing columns, duplicate keys,
                null keys, too many bad rows). The task raises and the DAG run fails.
  * warning   - individual rows are bad (impossible values). Those rows are quarantined to
                rejected.parquet with a reason, and the rest of the day still loads.
Every check result is written to the dq_results table, pass or fail, so quality is auditable.
"""

import logging
from dataclasses import dataclass, field
from datetime import date

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from skyflow import config
from skyflow.extract import partition_dir

log = logging.getLogger(__name__)

CRITICAL = "critical"
WARNING = "warning"

REQUIRED_COLUMNS = list(config.SOURCE_COLUMNS.values())
# Fields without which a row can't be identified or placed in the star schema.
CRITICAL_FIELDS = [
    "flight_date", "airline_code", "flight_number", "origin", "dest",
    "crs_dep_time", "crs_arr_time", "distance_miles", "is_cancelled", "is_diverted",
]
NUMERIC_COLUMNS = [
    "flight_number", "crs_dep_time", "dep_time", "dep_delay_min", "crs_arr_time", "arr_time",
    "arr_delay_min", "is_cancelled", "is_diverted", "sched_elapsed_min", "actual_elapsed_min",
    "distance_miles", "carrier_delay_min", "weather_delay_min", "nas_delay_min",
    "security_delay_min", "late_aircraft_delay_min",
]
HHMM_COLUMNS = ["crs_dep_time", "dep_time", "crs_arr_time", "arr_time"]


class DataQualityError(Exception):
    """Raised when a critical check fails, so the Airflow task (and run) fails loudly."""


@dataclass
class CheckResult:
    check_name: str
    severity: str
    passed: bool
    failed_rows: int = 0
    details: str = ""


@dataclass
class ValidationReport:
    flight_date: date
    total_rows: int
    results: list[CheckResult] = field(default_factory=list)
    # Per-row reject reason ("" = row is good); same index as the input frame.
    reject_reasons: pd.Series | None = None

    @property
    def critical_failures(self) -> list[CheckResult]:
        return [r for r in self.results if r.severity == CRITICAL and not r.passed]

    @property
    def rejected_rows(self) -> int:
        return 0 if self.reject_reasons is None else int((self.reject_reasons != "").sum())


def _is_valid_hhmm(values: pd.Series) -> pd.Series:
    """True for BTS-style HHMM times (0..2400, minutes < 60). NaN counts as valid here;
    nulls are judged separately by the null and status checks."""
    return values.isna() | ((values >= 0) & (values <= 2400) & (values % 100 < 60))


def validate_frame(df: pd.DataFrame, flight_date: date) -> ValidationReport:
    """Run all checks on a raw (all-string) partition. Pure function: no IO, easy to unit test."""
    report = ValidationReport(flight_date=flight_date, total_rows=len(df))
    add = report.results.append

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    add(CheckResult("required_columns", CRITICAL, not missing, details=f"missing={missing}"))
    if missing:
        return report  # nothing else is meaningful without the schema

    add(CheckResult("row_count_nonzero", CRITICAL, len(df) > 0, details=f"rows={len(df)}"))
    if df.empty:
        return report

    null_counts = df[CRITICAL_FIELDS].isna().sum()
    null_rows = int(df[CRITICAL_FIELDS].isna().any(axis=1).sum())
    add(CheckResult(
        "null_critical_fields", CRITICAL, null_rows == 0, null_rows,
        details=str(null_counts[null_counts > 0].to_dict()),
    ))

    dupes = int(df.duplicated(config.NATURAL_KEY, keep=False).sum())
    add(CheckResult(
        "duplicate_natural_key", CRITICAL, dupes == 0, dupes,
        details=f"key={config.NATURAL_KEY}",
    ))

    # ---- row-level checks: each produces a mask of bad rows -------------------------------
    num = df[NUMERIC_COLUMNS].apply(pd.to_numeric, errors="coerce")
    row_checks: dict[str, tuple[pd.Series, str]] = {}

    bad_type = (df[NUMERIC_COLUMNS].notna() & num.isna()).any(axis=1)
    row_checks["numeric_types"] = (bad_type, "non-numeric value in a numeric column")

    bad_time = ~pd.concat([_is_valid_hhmm(num[c]) for c in HHMM_COLUMNS], axis=1).all(axis=1)
    row_checks["time_format"] = (bad_time, "HHMM outside 0000-2400 or minutes >= 60")

    row_checks["distance_positive"] = (~(num["distance_miles"] > 0), "distance_miles <= 0")
    row_checks["sched_elapsed_positive"] = (
        ~(num["sched_elapsed_min"] > 0), "scheduled block time <= 0 or missing",
    )

    delay_cols = ["dep_delay_min", "arr_delay_min"]
    out_of_bounds = (
        (num[delay_cols] < config.MIN_DELAY_MIN) | (num[delay_cols] > config.MAX_DELAY_MIN)
    ).any(axis=1)
    row_checks["delay_bounds"] = (
        out_of_bounds, f"delay outside [{config.MIN_DELAY_MIN}, {config.MAX_DELAY_MIN}] min",
    )

    bad_code = ~(
        df["origin"].str.fullmatch(r"[A-Z]{3}", na=False)
        & df["dest"].str.fullmatch(r"[A-Z]{3}", na=False)
    )
    row_checks["airport_code_format"] = (bad_code, "airport code is not 3 uppercase letters")
    row_checks["origin_differs_dest"] = (df["origin"] == df["dest"], "origin == dest")

    # A flight that neither cancelled nor diverted must have arrived (arr_delay present), and a
    # cancelled flight must not have one. Flags must be 0/1.
    flown = (num["is_cancelled"] == 0) & (num["is_diverted"] == 0)
    inconsistent = (
        ~num["is_cancelled"].isin([0, 1]) | ~num["is_diverted"].isin([0, 1])
        | (flown & num["arr_delay_min"].isna())
        | ((num["is_cancelled"] == 1) & num["arr_delay_min"].notna())
    )
    row_checks["status_consistency"] = (inconsistent, "cancel/divert flags contradict times")

    reasons = pd.Series("", index=df.index)
    for name, (mask, description) in row_checks.items():
        mask = mask.fillna(True)
        n = int(mask.sum())
        add(CheckResult(name, WARNING, n == 0, n, details=description))
        reasons = reasons.where(~mask, reasons + name + ";")
    report.reject_reasons = reasons.str.rstrip(";")

    reject_rate = report.rejected_rows / len(df)
    add(CheckResult(
        "reject_rate", CRITICAL, reject_rate <= config.MAX_REJECT_RATE, report.rejected_rows,
        details=f"rate={reject_rate:.4%} max={config.MAX_REJECT_RATE:.2%}",
    ))
    return report


def save_results(
    engine: Engine, results: list[CheckResult], flight_date: date, run_id: str, stage: str
) -> None:
    """Write check results. Delete-then-insert per (run, date, stage) so a task retry doesn't
    double up rows, while results from earlier runs stay as history."""
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM dq_results WHERE run_id = :r AND flight_date = :d AND stage = :s"),
            {"r": run_id, "d": flight_date, "s": stage},
        )
        conn.execute(
            text("""
                INSERT INTO dq_results
                    (run_id, flight_date, stage, check_name, severity, passed, failed_rows, details)
                VALUES (:run_id, :flight_date, :stage, :check_name, :severity, :passed,
                        :failed_rows, :details)
            """),
            [
                {"run_id": run_id, "flight_date": flight_date, "stage": stage, **vars(r)}
                for r in results
            ],
        )


def raise_on_critical(results: list[CheckResult], flight_date: date) -> None:
    for r in results:
        level = logging.INFO if r.passed else (logging.ERROR if r.severity == CRITICAL else logging.WARNING)
        log.log(level, "[%s] %-24s passed=%s failed_rows=%s %s",
                r.severity, r.check_name, r.passed, r.failed_rows, r.details)
    failures = [r for r in results if r.severity == CRITICAL and not r.passed]
    if failures:
        names = ", ".join(r.check_name for r in failures)
        raise DataQualityError(f"{flight_date}: critical data-quality checks failed: {names}")


def reconcile(flight_date: date, run_id: str, engine: Engine) -> list[CheckResult]:
    """Post-load checks: did everything that passed validation actually land in the warehouse?"""
    part = partition_dir(flight_date)
    raw_rows = len(pd.read_parquet(part / "raw.parquet", columns=["flight_date"]))
    clean = pd.read_parquet(part / "clean.parquet", columns=["is_cancelled", "is_diverted"])
    flown = int((~clean["is_cancelled"] & ~clean["is_diverted"]).sum())
    rejected_path = part / "rejected.parquet"
    rejected = len(pd.read_parquet(rejected_path)) if rejected_path.exists() else 0

    with engine.connect() as conn:
        fact_rows = conn.execute(
            text("SELECT COUNT(*) FROM fact_flights WHERE date_key = :dk"),
            {"dk": int(flight_date.strftime("%Y%m%d"))},
        ).scalar()
        feature_rows = conn.execute(
            text("SELECT COUNT(*) FROM features_flight_delay WHERE flight_date = :d"),
            {"d": flight_date},
        ).scalar()

    results = [
        CheckResult(
            "row_count_reconciliation", CRITICAL, fact_rows == len(clean), abs(fact_rows - len(clean)),
            details=f"staged_clean={len(clean)} warehouse={fact_rows}",
        ),
        CheckResult(
            "raw_accounted_for", CRITICAL, raw_rows == len(clean) + rejected,
            abs(raw_rows - len(clean) - rejected),
            details=f"raw={raw_rows} clean={len(clean)} rejected={rejected}",
        ),
        # Features cover exactly the flights with a known outcome (not cancelled/diverted).
        CheckResult(
            "feature_row_count", CRITICAL, feature_rows == flown, abs(feature_rows - flown),
            details=f"flown={flown} features={feature_rows}",
        ),
    ]
    save_results(engine, results, flight_date, run_id, stage="reconcile")
    raise_on_critical(results, flight_date)
    return results


def validate(flight_date: date, run_id: str, engine: Engine) -> ValidationReport:
    """Validate the staged partition, split it into validated/rejected files, record results."""
    part = partition_dir(flight_date)
    df = pd.read_parquet(part / "raw.parquet")
    report = validate_frame(df, flight_date)

    if report.reject_reasons is not None:
        bad = report.reject_reasons != ""
        df[~bad].to_parquet(part / "validated.parquet", index=False)
        df[bad].assign(reject_reason=report.reject_reasons[bad]).to_parquet(
            part / "rejected.parquet", index=False
        )

    save_results(engine, report.results, flight_date, run_id, stage="validate")
    raise_on_critical(report.results, flight_date)
    log.info("%s: %s rows, %s rejected", flight_date, report.total_rows, report.rejected_rows)
    return report
