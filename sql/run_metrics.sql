-- Summary numbers for the README "Results" section. Run against the warehouse:
--   docker compose exec -T warehouse psql -U skyflow -d skyflow < sql/run_metrics.sql

\echo '== table sizes =='
SELECT 'fact_flights' AS table_name, COUNT(*) AS row_count FROM fact_flights
UNION ALL SELECT 'features_flight_delay', COUNT(*) FROM features_flight_delay
UNION ALL SELECT 'dim_airline', COUNT(*) FROM dim_airline
UNION ALL SELECT 'dim_airport', COUNT(*) FROM dim_airport
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL SELECT 'dim_time', COUNT(*) FROM dim_time
UNION ALL SELECT 'dq_results', COUNT(*) FROM dq_results;

\echo '== on-disk size =='
SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_catalog.pg_statio_user_tables ORDER BY pg_total_relation_size(relid) DESC;

\echo '== DQ: distinct checks, partitions covered, failures =='
SELECT COUNT(DISTINCT check_name) AS distinct_checks,
       COUNT(DISTINCT flight_date) AS partitions,
       COUNT(*) AS check_executions,
       SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) AS failed_executions
FROM dq_results;

\echo '== DQ: rows flagged per check (latest run per day) =='
WITH latest AS (
    SELECT DISTINCT ON (flight_date, stage, check_name) *
    FROM dq_results ORDER BY flight_date, stage, check_name, checked_at DESC
)
SELECT stage, check_name, severity, SUM(failed_rows) AS failed_rows,
       SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) AS days_failed
FROM latest GROUP BY stage, check_name, severity ORDER BY stage, severity, check_name;

\echo '== operations summary =='
SELECT COUNT(*) AS flights,
       SUM(is_cancelled::INT) AS cancelled,
       SUM(is_diverted::INT) AS diverted,
       ROUND(AVG(is_arr_delayed_15::INT)::NUMERIC, 4) AS delayed_15_rate,
       MIN(date_key) AS first_day,
       MAX(date_key) AS last_day
FROM fact_flights;
