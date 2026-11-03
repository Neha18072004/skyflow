# SkyFlow – Design Decisions

Why the pipeline is built the way it is, and what was traded away. Each section ends with the
alternative that was considered.

---

## 1. Data source

**BTS "Reporting Carrier On-Time Performance"** (US DOT), Jan–Mar 2024: one row per scheduled
domestic flight leg, 110 columns, ~550k rows per month.

- Public, no login, and has every field the model needs: scheduled vs actual times, delays,
  cancellation and diversion flags, distance, and delay-cause minutes.
- No equivalent flight-level dataset is public for Indian carriers (DGCA publishes monthly
  aggregates only). The pipeline doesn't depend on the source: swapping it means changing
  `config.SOURCE_COLUMNS`, `extract.py` and the airline lookup.
- We keep 27 of the 110 columns. The rest are mostly detail for up to five diversion airports,
  which we don't model.

## 2. Architecture: ELT, batch, partitioned by flight date

```
raw monthly CSV ─extract─▶ staging/flight_date=D/raw.parquet
                ─validate─▶ validated.parquet + rejected.parquet + dq_results
                ─transform─▶ clean.parquet
                ─load─────▶ dim_* upserts, fact_flights delete-insert
                ─features─▶ features_flight_delay (SQL, in-warehouse)
                ─dq_report─▶ reconciliation checks
```

- **One DAG run = one flight date.** The logical date (`ds`) *is* the partition key. That makes
  re-runs, retries and backfills the same operation: "process day D".
- **Staging files between tasks, not XCom.** XCom is for small metadata in Airflow's DB, not
  18k-row datasets. With files, every task can be retried or inspected on its own.
- **Parquet in staging:** typed, compressed and fast. Raw strings are kept in `raw.parquet`
  so validation sees exactly what the source sent.
- **Features computed in SQL, in the warehouse.** It's a set-based join and aggregate over
  history that already lives in Postgres, so there's no point pulling it into pandas.

*Alternative:* a single big job that loads the whole quarter. It's simpler, but you can't re-run
one day, and a bad day blocks everything.

## 3. Warehouse: star schema

**Grain of `fact_flights`: one row per scheduled flight leg**, identified by
(flight date, reporting airline, flight number, origin airport). This is the natural key. It is
enforced with a `UNIQUE` constraint, and it was verified to have zero duplicates in the source.

| Table | Type | Key | Notes |
|---|---|---|---|
| `fact_flights` | fact | `flight_key` (identity) | delays, flags, times, distance, delay causes |
| `dim_date` | dimension | `date_key` = YYYYMMDD | generated 2020–2030, not loaded from source |
| `dim_time` | dimension | `time_key` = HHMM | 1,440 minutes; hour, time block, day part |
| `dim_airline` | dimension | `airline_key` (identity) | natural key `airline_code`, SCD type 1 |
| `dim_airport` | dimension | `airport_key` (identity) | used twice: origin and destination (role-playing) |

- **Surrogate keys** keep the fact narrow (smallint/int instead of strings) and decouple the
  warehouse from source codes. If a carrier code were reused, only the dimension changes.
- **Smart keys for date/time** (20240115, 1435) are a common, deliberate exception: they are
  stable, human-readable, and let you filter the fact by date without a join.
- **SCD type 1** for airlines and airports: overwrite on change, no history. City names don't
  change in a meaningful way within the analysis window. *Alternative:* SCD2 with validity dates
  if, e.g., airline mergers needed tracking.
- **Indexes:** the unique key leads with `date_key`, so it also serves the per-day DELETE and
  date filters. Separate indexes cover airline, route (origin, dest) and destination, which are
  the main slice-and-dice dimensions.
- **Times are local wall-clock** (the source has no time zones). The arrival *date* is derived by
  choosing the day offset (−1/0/+1) that makes arrival − departure closest to the scheduled block
  time. The leftover error is the time-zone gap (≤ ~6h), far below the 24h between candidate
  days. A naive "arrival < departure ⇒ next day" rule misclassifies short westbound hops.
- **Actual times = scheduled + delay**, rather than parsing `DepTime` / `ArrTime`, which avoids
  midnight roll-over bugs.

*Not done (on purpose):* Postgres table partitioning. At ~1.7M rows an index on `date_key` is
enough. Monthly range partitions would be the next step at 10–100× the volume, and would let a
day's reload become a partition swap.

## 4. Data quality

Checks live in `validate.py` as pure functions over a DataFrame (easy to unit test). Every
result, pass or fail, is written to `dq_results` with run id, date, stage, severity and
failed-row count.

**Critical** (task raises → DAG run fails → nothing reaches the warehouse):
| check | why it's critical |
|---|---|
| `required_columns` | schema drift upstream; nothing downstream is meaningful |
| `row_count_nonzero` | an empty day almost always means a broken feed, not zero flights |
| `null_critical_fields` | can't identify or place the flight in the star schema |
| `duplicate_natural_key` | can't tell which copy is right; would double-count |
| `reject_rate` ≤ 1% | if many rows are bad, the *feed* is bad: fail rather than silently drop |
| `row_count_reconciliation` (post-load) | everything validated must land in `fact_flights` |
| `raw_accounted_for` (post-load) | raw = clean + rejected; no row lost silently |
| `feature_row_count` (post-load) | features exist for every flight with a known outcome |

**Warning / row-level** (bad rows quarantined to `rejected.parquet` with a reason; the day loads):
`numeric_types`, `time_format`, `distance_positive`, `sched_elapsed_positive`,
`delay_bounds` (−180 … 2,880 min), `airport_code_format`, `origin_differs_dest`,
`status_consistency` (e.g. not cancelled but no arrival).

**What actually got rejected (Jan–Mar 2024): 8 of 1,658,259 rows.** Six exceeded the 2,880-minute
delay bound (48–56 h, 5 of them American Airlines). These may be genuine multi-day delays rather
than errors, so the bound is a judgment call: it keeps extreme outliers out of averages and the
model, at the cost of 6 possibly-real flights. Raising `MAX_DELAY_MIN` would load them. The other
two had a missing or negative (−160 min) scheduled block time, which really is impossible.

The **1% reject-rate cap** is the bridge between the two. Individual bad rows are expected, and
quarantining them keeps the pipeline flowing. But a cap stops a systemic problem from being
"handled" by dropping half the data. This cap caught a real bug during development: an extract
change turned empty cells into the string `"nan"`, and 89% of rows failed type checks.

*Alternative:* Great Expectations or Soda. Both are worth it for many teams and many datasets. For
one source, ~15 checks in plain pandas are easier to read, test and explain.

## 5. Idempotent, incremental loads

- **Facts: delete-insert per partition in a single transaction.**
  `COPY` the day into a temp table → `DELETE FROM fact_flights WHERE date_key = D` →
  `INSERT … SELECT` with dimension-key lookups → `COMMIT`.
  - Re-running day D replaces it exactly. Readers never see a half-loaded day (MVCC).
  - Unlike `INSERT … ON CONFLICT DO UPDATE`, it also removes rows that disappeared from a
    corrected source file.
  - The natural-key `UNIQUE` constraint is a second line of defence.
- **Dimensions: `INSERT … ON CONFLICT DO UPDATE … WHERE IS DISTINCT FROM`**, which skips no-op
  updates so unchanged rows aren't rewritten daily.
- **Features: delete-insert per date.** The feature table's FK to `fact_flights` is
  `ON DELETE CASCADE`, so reloading a day's facts can't leave orphaned feature rows.
- **dq_results: delete-insert per (run_id, date, stage).** Task retries don't duplicate rows,
  but history across runs is kept.
- **Backfill** = a range of DAG runs (`catchup=True` with start/end dates, or
  `airflow dags backfill -s … -e …`). There is no separate code path.

Tested by `tests/test_load_idempotency.py`: same partition twice → identical counts; a corrected
re-run replaces the day and leaves other days untouched.

## 6. Orchestration (Airflow)

- `extract → validate → transform → load_dimensions → load_facts → build_features → dq_report`.
- The DAG file only orchestrates. Each task is a 2-line call into `skyflow.pipeline`, so the
  same logic runs from the CLI and in tests. Heavy imports happen inside tasks to keep the
  scheduler's DAG parsing fast.
- `retries=2`, `retry_delay=1 min`, `execution_timeout=10 min` per task, `dagrun_timeout=1h`.
  A day normally takes seconds, so a 10-minute task is a hang (e.g. a lock), not slowness.
- **`max_active_runs=4` with `depends_on_past=True` on `build_features` only.** Loading is
  independent per day and can run in parallel. Features for day D need facts for days < D.
  Requiring the previous day's `build_features` to have succeeded guarantees, by induction, that
  all earlier days are loaded, without serializing the whole DAG.
  - *Trade-off:* a failed day blocks feature builds for later days until it's fixed. That is the
    desired behaviour: features computed on missing history would be silently wrong.
  - *Known limitation:* re-loading an *old* day doesn't automatically refresh later days'
    rolling features. Clear `build_features` for the following 28 days, or run with
    "downstream + future".
- **LocalExecutor, no Celery/Redis.** One machine, one DAG. Celery adds a broker and workers for
  horizontal scale this workload doesn't need.
- **Separate Postgres for Airflow metadata vs the warehouse.** They have different lifecycles,
  backup needs and load profiles. Wiping the warehouse must not wipe run history, and vice versa.

## 7. Feature table and leakage prevention

`features_flight_delay`: one row per flight with a known outcome (not cancelled or diverted).
Target `is_arr_delayed_15` = arrival delay ≥ 15 min (the DOT definition, identical to BTS's own
`ArrDel15`).

Leakage rules:
1. **Historical rates use only days strictly before the flight date** (window D−28 … D−1).
   Same-day flights are excluded even if they landed earlier, because at prediction time their
   outcome may not be known yet.
2. **Only schedule-time features.** `dep_hour` comes from the *scheduled* departure. Actual
   departure time, departure delay, taxi times and delay-cause minutes are never features: they
   are only known after (or are part of) the outcome.
3. **History counts alongside rates** (`route_flights_28d`) so a model can discount a rate built
   from 2 flights. Rates are `NULL`, not 0, when there is no history, because "unknown" is not
   "never delayed".
4. **Time-based train/test split** in the baseline (train late Jan–Mar 10, test Mar 11–31). The
   first 28 days are excluded from training because their history windows are incomplete.

`tests/test_load_idempotency.py::test_historical_rates_use_only_prior_days` proves rule 1: day 2
is 100% late, yet its features show exactly day 1's 25% rate.

*Why 28 days?* It's four whole weeks, so every weekday is equally represented, and it's recent
enough to track schedule and seasonal shifts. *Alternative:* an expanding window (all history),
which is smoother but slower to react and costlier to compute as history grows.

## 8. Tooling choices

- **psycopg2, not psycopg 3.** Airflow 2.x pins SQLAlchemy < 2.0, which has no psycopg 3
  dialect. Local, CI and container versions are pinned identically (`requirements.txt` mirrors the
  Airflow image), so tests exercise the same library versions as production.
- **Stock `apache/airflow:2.10.5-python3.11` image, no custom build.** It already ships pandas,
  pyarrow, SQLAlchemy and psycopg2, and it's multi-arch (native arm64 on Apple Silicon).
- **CI** runs ruff and pytest against a real Postgres 16 service container. Integration tests
  skip locally without a database but *fail* in CI (`CI=true`), so a broken service can't turn
  the build green.
