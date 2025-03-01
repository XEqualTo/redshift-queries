-- This query retrieves concurrency scaling usage details from Amazon Redshift.
-- It aggregates the total number of queries and total usage time in seconds per hour.

SELECT 
    date_trunc('hour', end_time) AS burst_hour,  -- Groups data by hour
    SUM(queries) AS query_count,                 -- Counts the total number of queries per hour
    SUM(usage_in_seconds) AS burst_usage_in_seconds  -- Sums the concurrency scaling usage time in seconds per hour
FROM svcs_concurrency_scaling_usage
GROUP BY burst_hour;  -- Groups results by truncated hour
