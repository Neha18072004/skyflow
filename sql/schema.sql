-- SkyFlow warehouse: star schema for US domestic flight operations.
-- Grain of fact_flights: one row per scheduled flight leg
--   (flight date, marketing airline, flight number, origin airport).
-- Every statement is idempotent (IF NOT EXISTS / ON CONFLICT) so this file can be re-applied safely.

-- ---------------------------------------------------------------------------
-- Dimensions
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_date (
    date_key      INTEGER PRIMARY KEY,          -- YYYYMMDD: readable, and sorts like the date
    full_date     DATE        NOT NULL UNIQUE,
    year          SMALLINT    NOT NULL,
    quarter       SMALLINT    NOT NULL,
    month         SMALLINT    NOT NULL,
    month_name    VARCHAR(9)  NOT NULL,
    day_of_month  SMALLINT    NOT NULL,
    day_of_week   SMALLINT    NOT NULL,         -- ISO: 1 = Monday ... 7 = Sunday (same as BTS)
    day_name      VARCHAR(9)  NOT NULL,
    week_of_year  SMALLINT    NOT NULL,
    is_weekend    BOOLEAN     NOT NULL
);

-- dim_date is generated, not loaded from source: every date exists even if no flights flew.
INSERT INTO dim_date
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d::DATE,
    EXTRACT(YEAR FROM d),
    EXTRACT(QUARTER FROM d),
    EXTRACT(MONTH FROM d),
    TRIM(TO_CHAR(d, 'Month')),
    EXTRACT(DAY FROM d),
    EXTRACT(ISODOW FROM d),
    TRIM(TO_CHAR(d, 'Day')),
    EXTRACT(WEEK FROM d),
    EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, INTERVAL '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_time (
    time_key     SMALLINT PRIMARY KEY,          -- HHMM as an integer, e.g. 1435 = 14:35
    hour         SMALLINT    NOT NULL,
    minute       SMALLINT    NOT NULL,
    time_block   CHAR(9)     NOT NULL,          -- hourly block, e.g. '1400-1459' (BTS convention)
    day_part     VARCHAR(10) NOT NULL           -- night / morning / afternoon / evening
);

INSERT INTO dim_time
SELECT
    (m / 60) * 100 + (m % 60),
    m / 60,
    m % 60,
    LPAD(((m / 60) * 100)::TEXT, 4, '0') || '-' || LPAD(((m / 60) * 100 + 59)::TEXT, 4, '0'),
    CASE
        WHEN m / 60 < 6  THEN 'night'
        WHEN m / 60 < 12 THEN 'morning'
        WHEN m / 60 < 18 THEN 'afternoon'
        ELSE 'evening'
    END
FROM generate_series(0, 1439) AS m
ON CONFLICT (time_key) DO NOTHING;

-- Airlines and airports are upserted from source data on every load (SCD type 1: the latest
-- name wins, no history kept, which is fine for attributes that rarely change).
CREATE TABLE IF NOT EXISTS dim_airline (
    airline_key   SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    airline_code  VARCHAR(3)   NOT NULL UNIQUE,  -- natural key (IATA reporting carrier code)
    airline_name  VARCHAR(100) NOT NULL,
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- One airport dimension, used twice by the fact table (origin and destination role-playing).
CREATE TABLE IF NOT EXISTS dim_airport (
    airport_key   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    airport_code  CHAR(3)      NOT NULL UNIQUE,  -- natural key (IATA airport code)
    city_name     VARCHAR(100) NOT NULL,
    state_code    CHAR(2),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Fact
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_flights (
    flight_key            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_key              INTEGER  NOT NULL REFERENCES dim_date (date_key),
    airline_key           SMALLINT NOT NULL REFERENCES dim_airline (airline_key),
    origin_airport_key    INTEGER  NOT NULL REFERENCES dim_airport (airport_key),
    dest_airport_key      INTEGER  NOT NULL REFERENCES dim_airport (airport_key),
    sched_dep_time_key    SMALLINT NOT NULL REFERENCES dim_time (time_key),
    sched_arr_time_key    SMALLINT NOT NULL REFERENCES dim_time (time_key),
    flight_number         INTEGER  NOT NULL,
    tail_number           VARCHAR(10),
    -- Local wall-clock times at each airport; the source has no time-zone information.
    sched_dep_at          TIMESTAMP NOT NULL,
    actual_dep_at         TIMESTAMP,
    sched_arr_at          TIMESTAMP NOT NULL,
    actual_arr_at         TIMESTAMP,
    dep_delay_min         SMALLINT,             -- negative = early; NULL if it never departed
    arr_delay_min         SMALLINT,             -- NULL when cancelled or diverted
    is_arr_delayed_15     BOOLEAN,              -- NULL when cancelled or diverted
    is_cancelled          BOOLEAN  NOT NULL,
    cancellation_code     CHAR(1),              -- A carrier, B weather, C NAS, D security
    is_diverted           BOOLEAN  NOT NULL,
    distance_miles        SMALLINT NOT NULL,
    sched_elapsed_min     SMALLINT NOT NULL,
    actual_elapsed_min    SMALLINT,
    carrier_delay_min     SMALLINT,             -- delay-cause minutes: only reported when arr delay >= 15
    weather_delay_min     SMALLINT,
    nas_delay_min         SMALLINT,
    security_delay_min    SMALLINT,
    late_aircraft_delay_min SMALLINT,
    batch_id              VARCHAR(250) NOT NULL, -- Airflow run id (or 'manual') that loaded the row
    loaded_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    -- Natural key: guarantees a re-run can never double-count a flight even if the
    -- delete-insert logic were bypassed. Leading date_key also serves partition deletes.
    CONSTRAINT uq_fact_flights_natural
        UNIQUE (date_key, airline_key, flight_number, origin_airport_key)
);

CREATE INDEX IF NOT EXISTS ix_fact_flights_airline ON fact_flights (airline_key);
CREATE INDEX IF NOT EXISTS ix_fact_flights_route   ON fact_flights (origin_airport_key, dest_airport_key);
CREATE INDEX IF NOT EXISTS ix_fact_flights_dest    ON fact_flights (dest_airport_key);

-- ---------------------------------------------------------------------------
-- Data quality results (one row per check per partition per run)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dq_results (
    dq_result_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id          VARCHAR(250) NOT NULL,
    flight_date     DATE         NOT NULL,
    stage           VARCHAR(20)  NOT NULL,      -- 'validate' (pre-load) or 'reconcile' (post-load)
    check_name      VARCHAR(100) NOT NULL,
    severity        VARCHAR(10)  NOT NULL CHECK (severity IN ('critical', 'warning')),
    passed          BOOLEAN      NOT NULL,
    failed_rows     INTEGER      NOT NULL DEFAULT 0,
    details         TEXT,
    checked_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_dq_results_date ON dq_results (flight_date, stage);

-- ---------------------------------------------------------------------------
-- ML feature table (populated per partition by sql/features.sql)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS features_flight_delay (
    -- CASCADE: re-loading a day's facts drops that day's features, which build_features rebuilds.
    flight_key               BIGINT PRIMARY KEY REFERENCES fact_flights (flight_key) ON DELETE CASCADE,
    flight_date              DATE        NOT NULL,
    airline_code             VARCHAR(3)  NOT NULL,
    origin                   CHAR(3)     NOT NULL,
    dest                     CHAR(3)     NOT NULL,
    dep_hour                 SMALLINT    NOT NULL,   -- from the SCHEDULED departure (known in advance)
    day_of_week              SMALLINT    NOT NULL,
    month                    SMALLINT    NOT NULL,
    distance_miles           SMALLINT    NOT NULL,
    route_flights_28d        INTEGER     NOT NULL,   -- history volume: lets a model discount sparse rates
    route_delay_rate_28d     REAL,                   -- NULL when the route has no prior history
    airline_flights_28d      INTEGER     NOT NULL,
    airline_delay_rate_28d   REAL,
    is_arr_delayed_15        BOOLEAN     NOT NULL    -- target
);

CREATE INDEX IF NOT EXISTS ix_features_flight_date ON features_flight_delay (flight_date);
