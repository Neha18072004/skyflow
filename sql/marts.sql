-- Rebuild every mart from the local warehouse (full refresh: marts are small, so recomputing them
-- is simpler and safer than tracking which days changed). Runs in one transaction from publish.py.

TRUNCATE marts.daily_ops;
INSERT INTO marts.daily_ops
SELECT d.full_date, al.airline_code, al.airline_name, o.airport_code,
       COUNT(*),
       COUNT(f.is_arr_delayed_15),
       COUNT(*) FILTER (WHERE f.is_arr_delayed_15),
       COUNT(*) FILTER (WHERE f.is_cancelled),
       COUNT(*) FILTER (WHERE f.is_diverted),
       COALESCE(SUM(f.arr_delay_min) FILTER (WHERE f.is_arr_delayed_15 IS NOT NULL), 0),
       COALESCE(SUM(f.carrier_delay_min), 0),
       COALESCE(SUM(f.weather_delay_min), 0),
       COALESCE(SUM(f.nas_delay_min), 0),
       COALESCE(SUM(f.security_delay_min), 0),
       COALESCE(SUM(f.late_aircraft_delay_min), 0)
FROM fact_flights f
JOIN dim_date d     ON d.date_key = f.date_key
JOIN dim_airline al ON al.airline_key = f.airline_key
JOIN dim_airport o  ON o.airport_key = f.origin_airport_key
GROUP BY d.full_date, al.airline_code, al.airline_name, o.airport_code;

TRUNCATE marts.delay_heatmap;
INSERT INTO marts.delay_heatmap
SELECT al.airline_code, o.airport_code, d.day_of_week, t.hour,
       COUNT(*), COUNT(*) FILTER (WHERE f.is_arr_delayed_15)
FROM fact_flights f
JOIN dim_date d     ON d.date_key = f.date_key
JOIN dim_time t     ON t.time_key = f.sched_dep_time_key
JOIN dim_airline al ON al.airline_key = f.airline_key
JOIN dim_airport o  ON o.airport_key = f.origin_airport_key
WHERE f.is_arr_delayed_15 IS NOT NULL
GROUP BY al.airline_code, o.airport_code, d.day_of_week, t.hour;

TRUNCATE marts.route_daily;
INSERT INTO marts.route_daily
WITH top_routes AS (
    SELECT origin_airport_key, dest_airport_key
    FROM fact_flights
    GROUP BY origin_airport_key, dest_airport_key
    ORDER BY COUNT(*) DESC
    LIMIT 300
)
SELECT d.full_date, al.airline_code, o.airport_code, de.airport_code,
       COUNT(*),
       COUNT(f.is_arr_delayed_15),
       COUNT(*) FILTER (WHERE f.is_arr_delayed_15),
       COUNT(*) FILTER (WHERE f.is_cancelled),
       COALESCE(SUM(f.arr_delay_min) FILTER (WHERE f.is_arr_delayed_15 IS NOT NULL), 0)
FROM fact_flights f
JOIN top_routes r   USING (origin_airport_key, dest_airport_key)
JOIN dim_date d     ON d.date_key = f.date_key
JOIN dim_airline al ON al.airline_key = f.airline_key
JOIN dim_airport o  ON o.airport_key = f.origin_airport_key
JOIN dim_airport de ON de.airport_key = f.dest_airport_key
GROUP BY d.full_date, al.airline_code, o.airport_code, de.airport_code;

TRUNCATE marts.dq_summary;
INSERT INTO marts.dq_summary
SELECT DISTINCT ON (flight_date, stage, check_name)
       flight_date, stage, check_name, severity, passed, failed_rows, details, checked_at
FROM dq_results
ORDER BY flight_date, stage, check_name, checked_at DESC;

TRUNCATE marts.pipeline_summary;
INSERT INTO marts.pipeline_summary (
    first_date, last_date, partitions, rows_ingested, rows_loaded, rows_rejected, feature_rows,
    dq_checks, dq_results, critical_failures, last_loaded_at
)
SELECT
    (SELECT MIN(flight_date) FROM marts.dq_summary),
    (SELECT MAX(flight_date) FROM marts.dq_summary),
    (SELECT COUNT(DISTINCT flight_date) FROM marts.dq_summary),
    (SELECT COUNT(*) FROM fact_flights)
        + (SELECT COALESCE(SUM(failed_rows), 0) FROM marts.dq_summary WHERE check_name = 'reject_rate'),
    (SELECT COUNT(*) FROM fact_flights),
    (SELECT COALESCE(SUM(failed_rows), 0) FROM marts.dq_summary WHERE check_name = 'reject_rate'),
    (SELECT COUNT(*) FROM features_flight_delay),
    (SELECT COUNT(DISTINCT check_name) FROM dq_results),
    (SELECT COUNT(*) FROM dq_results),
    (SELECT COUNT(*) FROM marts.dq_summary WHERE severity = 'critical' AND NOT passed),
    (SELECT MAX(loaded_at) FROM fact_flights);
