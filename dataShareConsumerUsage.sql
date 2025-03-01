-- This query analyzes consumer usage of shared data in Amazon Redshift.
-- It tracks user activity, request durations, query execution times, and error counts.
-- The query consists of multiple CTEs:
-- 1. `consumer_activity`: Aggregates consumer activity, tracking request start/end times, 
--    unique transactions, and request errors.
-- 2. `consumer_query`: Joins consumer activity with query execution details to analyze request
--    intervals, execution times, and total time spent.
-- 3. `consumer_query_aggregate`: Computes aggregate metrics such as total/average request duration, 
--    execution times, and transaction counts.
-- 4. `consumer_query_request_percentile`: Calculates request duration percentiles (80th, 90th, 99th).
-- Finally, it joins aggregated data with percentiles to generate insights on consumer usage.

WITH consumer_activity AS 
(
    SELECT uc.userid,
           u.usename AS db_username,
           uc.pid,
           uc.xid,
           MIN(uc.recordtime) AS request_start_date,
           MAX(uc.recordtime) AS request_end_date,
           DATEDIFF('milliseconds', MIN(uc.recordtime), MAX(uc.recordtime))::NUMERIC(38,4) / 1000 AS request_duration_secs,
           NVL(COUNT(DISTINCT uc.transaction_uid), 0) AS unique_transaction,
           NVL(COUNT(uc.request_id), 0) AS total_usage_consumer_count,
           SUM(CASE WHEN TRIM(uc.error) = '' THEN 0 ELSE 1 END) AS request_error_count
    FROM svl_datashare_usage_consumer uc
    INNER JOIN pg_user u ON (u.usesysid = uc.userid)
    GROUP BY 1,2,3,4
),
consumer_query AS (
    SELECT TRIM(q."database") AS dbname,
           TRIM(cu.db_username) AS db_username,
           cu.request_start_date::DATE AS request_date,
           cu.request_duration_secs,
           DATEDIFF('milliseconds', cu.request_end_date, q.starttime)::NUMERIC(38,4) / 1000 AS request_interval_secs,
           DATEDIFF('milliseconds', q.starttime, q.endtime)::NUMERIC(38,4) / 1000 AS query_execution_secs,
           DATEDIFF('milliseconds', request_start_date, q.endtime)::NUMERIC(38,4) / 1000 AS total_execution_secs,
           q.query,
           cu.unique_transaction,
           cu.total_usage_consumer_count,
           cu.request_error_count
    FROM consumer_activity cu 
    INNER JOIN stl_query q ON (q.xid = cu.xid AND q.pid = cu.pid AND q.userid = cu.userid)
),
consumer_query_aggregate AS (
    SELECT cq.request_date,
           cq.dbname,
           cq.db_username,
           AVG(cq.request_duration_secs) AS avg_request_duration_secs,
           SUM(cq.request_duration_secs) AS total_request_duration_secs,
           AVG(cq.request_interval_secs) AS avg_request_interval_secs,
           SUM(cq.request_interval_secs) AS total_request_interval_secs,
           AVG(cq.query_execution_secs) AS avg_query_execution_secs,
           SUM(cq.query_execution_secs) AS total_query_execution_secs,
           AVG(cq.total_execution_secs) AS avg_execution_secs,
           SUM(cq.total_execution_secs) AS total_execution_secs,
           COUNT(cq.query) AS query_count,
           SUM(cq.unique_transaction) AS total_unique_transaction,
           SUM(cq.total_usage_consumer_count) AS total_usage_consumer_count,
           SUM(cq.request_error_count) AS total_request_error_count 
    FROM consumer_query cq
    GROUP BY 1,2,3
),
consumer_query_request_percentile AS (
    SELECT cq.request_date,
           cq.dbname,
           cq.db_username,
           PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY request_duration_secs) AS p80_request_sec,
           PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY request_duration_secs) AS p90_request_sec,
           PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY request_duration_secs) AS p99_request_sec
    FROM consumer_query cq
    GROUP BY 1,2,3
)
SELECT cqa.request_date,
       cqa.dbname,
       cqa.db_username,
       cqa.query_count,
       cqa.avg_query_execution_secs,
       cqa.total_query_execution_secs,
       cqa.avg_execution_secs,
       cqa.total_execution_secs,
       cqa.avg_request_duration_secs,
       cqrp.p80_request_sec,
       cqrp.p90_request_sec,
       cqrp.p99_request_sec,
       cqa.total_request_duration_secs,
       cqa.avg_request_interval_secs,
       cqa.total_request_interval_secs,  
       cqa.total_unique_transaction,
       cqa.total_usage_consumer_count,
       cqa.total_request_error_count
FROM consumer_query_aggregate cqa
INNER JOIN consumer_query_request_percentile cqrp 
    ON (cqa.request_date = cqrp.request_date AND cqa.dbname = cqrp.dbname AND cqa.db_username = cqrp.db_username)
ORDER BY 1,2,3;
