-- Rebuild features_flight_delay for ONE flight date (delete-insert, so re-runs are idempotent).
--
-- Leakage rule: every historical statistic for a flight on day D is computed only from flights
-- on days D-28 .. D-1. Nothing from day D itself is used, not even flights that landed earlier
-- that morning, because at prediction time (before departure) a same-day outcome may not be
-- known yet and using it would make offline metrics look better than production.
--
-- Parameters: :day_key (YYYYMMDD of D), :history_start_key (D-28), :flight_date (D)

DELETE FROM features_flight_delay WHERE flight_date = :flight_date;

INSERT INTO features_flight_delay (
    flight_key, flight_date, airline_code, origin, dest, dep_hour, day_of_week, month,
    distance_miles, route_flights_28d, route_delay_rate_28d, airline_flights_28d,
    airline_delay_rate_28d, is_arr_delayed_15
)
WITH history AS (
    SELECT airline_key, origin_airport_key, dest_airport_key, is_arr_delayed_15
    FROM fact_flights
    WHERE date_key >= :history_start_key
      AND date_key <  :day_key                -- strictly before D
      AND is_arr_delayed_15 IS NOT NULL       -- cancelled/diverted flights have no outcome
),
route_stats AS (
    SELECT origin_airport_key, dest_airport_key,
           COUNT(*) AS n,
           AVG(is_arr_delayed_15::INT) AS delay_rate
    FROM history
    GROUP BY origin_airport_key, dest_airport_key
),
airline_stats AS (
    SELECT airline_key,
           COUNT(*) AS n,
           AVG(is_arr_delayed_15::INT) AS delay_rate
    FROM history
    GROUP BY airline_key
)
SELECT
    f.flight_key,
    d.full_date,
    al.airline_code,
    o.airport_code,
    de.airport_code,
    t.hour,                                   -- scheduled hour: known before the flight
    d.day_of_week,
    d.month,
    f.distance_miles,
    COALESCE(rs.n, 0),
    rs.delay_rate,                            -- NULL = no history (new route / first days)
    COALESCE(ast.n, 0),
    ast.delay_rate,
    f.is_arr_delayed_15
FROM fact_flights f
JOIN dim_date    d  ON d.date_key     = f.date_key
JOIN dim_time    t  ON t.time_key     = f.sched_dep_time_key
JOIN dim_airline al ON al.airline_key = f.airline_key
JOIN dim_airport o  ON o.airport_key  = f.origin_airport_key
JOIN dim_airport de ON de.airport_key = f.dest_airport_key
LEFT JOIN route_stats   rs  ON rs.origin_airport_key = f.origin_airport_key
                           AND rs.dest_airport_key   = f.dest_airport_key
LEFT JOIN airline_stats ast ON ast.airline_key = f.airline_key
WHERE f.date_key = :day_key
  AND f.is_arr_delayed_15 IS NOT NULL;
