-- Dashboard marts: small pre-aggregated tables, the only thing published to the cloud database.
-- Applied to BOTH the local warehouse (where marts.sql fills them) and the cloud Postgres.
-- Grains are chosen so the dashboard's filters (date range, airline, origin) apply to every chart
-- while the whole set stays a few tens of MB instead of the 414 MB fact table.

CREATE SCHEMA IF NOT EXISTS marts;

-- KPIs, daily trend, airline league table, delay causes. Grain: date x airline x origin.
CREATE TABLE IF NOT EXISTS marts.daily_ops (
    flight_date         DATE        NOT NULL,
    airline_code        VARCHAR(3)  NOT NULL,
    airline_name        VARCHAR(100) NOT NULL,
    origin              CHAR(3)     NOT NULL,
    flights             INTEGER     NOT NULL,   -- scheduled
    flown               INTEGER     NOT NULL,   -- not cancelled or diverted (has an outcome)
    delayed_15          INTEGER     NOT NULL,   -- flown and arrived 15+ min late
    cancelled           INTEGER     NOT NULL,
    diverted            INTEGER     NOT NULL,
    arr_delay_sum       INTEGER     NOT NULL,   -- sum over flown flights; avg = sum / flown
    carrier_delay_min   INTEGER     NOT NULL,   -- delay-cause minutes (reported when 15+ min late)
    weather_delay_min   INTEGER     NOT NULL,
    nas_delay_min       INTEGER     NOT NULL,
    security_delay_min  INTEGER     NOT NULL,
    late_aircraft_delay_min INTEGER NOT NULL,
    PRIMARY KEY (flight_date, airline_code, origin)
);

-- Hour x weekday heatmap over the whole loaded period (airline and origin filters apply, the
-- date filter doesn't). Adding a date or month dimension would make this the largest mart.
CREATE TABLE IF NOT EXISTS marts.delay_heatmap (
    airline_code    VARCHAR(3)  NOT NULL,
    origin          CHAR(3)     NOT NULL,
    day_of_week     SMALLINT    NOT NULL,       -- ISO 1 = Monday
    dep_hour        SMALLINT    NOT NULL,       -- scheduled departure hour
    flown           INTEGER     NOT NULL,
    delayed_15      INTEGER     NOT NULL,
    PRIMARY KEY (airline_code, origin, day_of_week, dep_hour)
);

-- Route rankings, limited to the 300 busiest routes (origin-dest pairs) by scheduled flights.
CREATE TABLE IF NOT EXISTS marts.route_daily (
    flight_date     DATE        NOT NULL,
    airline_code    VARCHAR(3)  NOT NULL,
    origin          CHAR(3)     NOT NULL,
    dest            CHAR(3)     NOT NULL,
    flights         INTEGER     NOT NULL,
    flown           INTEGER     NOT NULL,
    delayed_15      INTEGER     NOT NULL,
    cancelled       INTEGER     NOT NULL,
    arr_delay_sum   INTEGER     NOT NULL,
    PRIMARY KEY (flight_date, airline_code, origin, dest)
);

-- Latest DQ result per partition and check.
CREATE TABLE IF NOT EXISTS marts.dq_summary (
    flight_date     DATE         NOT NULL,
    stage           VARCHAR(20)  NOT NULL,
    check_name      VARCHAR(100) NOT NULL,
    severity        VARCHAR(10)  NOT NULL,
    passed          BOOLEAN      NOT NULL,
    failed_rows     INTEGER      NOT NULL,
    details         TEXT,
    checked_at      TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (flight_date, stage, check_name)
);

-- One row: headline pipeline numbers for the Pipeline Health tab.
CREATE TABLE IF NOT EXISTS marts.pipeline_summary (
    first_date      DATE        NOT NULL,
    last_date       DATE        NOT NULL,
    partitions      INTEGER     NOT NULL,
    rows_ingested   INTEGER     NOT NULL,       -- raw rows = loaded + rejected
    rows_loaded     INTEGER     NOT NULL,
    rows_rejected   INTEGER     NOT NULL,
    feature_rows    INTEGER     NOT NULL,
    dq_checks       INTEGER     NOT NULL,       -- distinct checks
    dq_results      INTEGER     NOT NULL,       -- check executions recorded
    critical_failures INTEGER   NOT NULL,       -- latest result per day/check
    last_loaded_at  TIMESTAMPTZ NOT NULL,
    published_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
