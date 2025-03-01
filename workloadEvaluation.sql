/*
This SQL query analyzes query execution metrics from Amazon Redshift system tables (`stl_query`, `stl_scan`, `stl_wlm_query`) 
to derive workload patterns and utilization statistics. 

### Breakdown:
1. `hour_list` - Generates hourly time slots for the past 7 days.
2. `scan_sum` - Aggregates scanned bytes per query and segment.
3. `scan_list` - Determines the maximum scanned bytes per query.
4. `query_list` - Combines query execution times and assigns a workload size (`small`, `medium`, `large`).
5. `workload_exec_seconds` - Computes total, average, min, and max execution times for each workload size.
6. `query_list_2` - Maps queries to their respective hourly slots.
7. `hour_list_agg` - Aggregates the number of queries executed per hour by workload size.
8. `utilization_perc` - Computes daily workload activity percentages.
9. `activity_perc` - Computes average query activity percentage by workload size.
10. `mincount` - Computes the total number of minutes queries were executed daily.
11. `avgmincount` - Computes the average query execution time in minutes per workload size.
12. `final_output` - Combines all computed metrics into a summary report.

The final output provides insights into:
- The proportion of workload by size.
- Query execution times (total, average, min, max).
- Workload utilization and query activity percentages.
- The distribution of query execution times across workload sizes.
*/

WITH hour_list AS (
    SELECT DATE_TRUNC('m', starttime) AS start_hour,
           DATEADD('m', 1, start_hour) AS end_hour
    FROM stl_query q
    WHERE starttime >= (GETDATE() - 7)
    GROUP BY 1
),
scan_sum AS (
    SELECT query,
           segment,
           SUM(bytes) AS bytes
    FROM stl_scan
    WHERE userid > 1
    GROUP BY query, segment
),
scan_list AS (
    SELECT query,
           MAX(bytes) AS max_scan_bytes
    FROM scan_sum
    GROUP BY query
),
query_list AS (
    SELECT w.query,
           exec_start_time,
           exec_end_time,
           ROUND(total_exec_time / 1000 / 1000.0, 3) AS exec_sec,
           max_scan_bytes,
           CASE
               WHEN max_scan_bytes < 100000000 THEN 'small'
               WHEN max_scan_bytes BETWEEN 100000000 AND 500000000000 THEN 'medium'
               WHEN max_scan_bytes > 500000000000 THEN 'large'
           END AS size_type
    FROM stl_wlm_query w
    JOIN scan_list sc ON sc.query = w.query
),
workload_exec_seconds AS (
    SELECT 
        COUNT(*) AS query_cnt,
        SUM(CASE WHEN size_type = 'small' THEN exec_sec ELSE 0 END) AS small_workload_exec_sec_sum,
        SUM(CASE WHEN size_type = 'medium' THEN exec_sec ELSE 0 END) AS medium_workload_exec_sec_sum,
        SUM(CASE WHEN size_type = 'large' THEN exec_sec ELSE 0 END) AS large_workload_exec_sec_sum,
        
        AVG(CASE WHEN size_type = 'small' THEN exec_sec ELSE 0 END) AS small_workload_exec_sec_avg,
        AVG(CASE WHEN size_type = 'medium' THEN exec_sec ELSE 0 END) AS medium_workload_exec_sec_avg,
        AVG(CASE WHEN size_type = 'large' THEN exec_sec ELSE 0 END) AS large_workload_exec_sec_avg,
        
        MAX(CASE WHEN size_type = 'small' THEN exec_sec ELSE 0 END) AS small_workload_exec_sec_max,
        MAX(CASE WHEN size_type = 'medium' THEN exec_sec ELSE 0 END) AS medium_workload_exec_sec_max,
        MAX(CASE WHEN size_type = 'large' THEN exec_sec ELSE 0 END) AS large_workload_exec_sec_max,
        
        MIN(CASE WHEN size_type = 'small' THEN exec_sec ELSE 0 END) AS small_workload_exec_sec_min,
        MIN(CASE WHEN size_type = 'medium' THEN exec_sec ELSE 0 END) AS medium_workload_exec_sec_min,
        MIN(CASE WHEN size_type = 'large' THEN exec_sec ELSE 0 END) AS large_workload_exec_sec_min,
        
        AVG(CASE WHEN size_type = 'small' THEN max_scan_bytes ELSE 0 END) AS small_workload_max_scan_bytes_avg,
        AVG(CASE WHEN size_type = 'medium' THEN max_scan_bytes ELSE 0 END) AS medium_workload_max_scan_bytes_avg,
        AVG(CASE WHEN size_type = 'large' THEN max_scan_bytes ELSE 0 END) AS large_workload_max_scan_bytes_avg,

        (small_workload_exec_sec_sum + medium_workload_exec_sec_sum + large_workload_exec_sec_sum) AS total_workload_exec_sec_sum,
        small_workload_exec_sec_sum / (total_workload_exec_sec_sum * 1.00) AS Small_workload_perc,
        medium_workload_exec_sec_sum / (total_workload_exec_sec_sum * 1.00) AS Medium_workload_perc,
        large_workload_exec_sec_sum / (total_workload_exec_sec_sum * 1.00) AS Large_workload_perc
    FROM query_list
),
query_list_2 AS (
    SELECT 
        start_hour,
        query,
        size_type,
        max_scan_bytes,
        exec_sec,
        exec_start_time,
        exec_end_time
    FROM hour_list h
    JOIN query_list q ON 
        exec_start_time BETWEEN start_hour AND end_hour
        OR exec_end_time BETWEEN start_hour AND end_hour
        OR (exec_start_time < start_hour AND exec_end_time > end_hour)
),
hour_list_agg AS (
    SELECT 
        start_hour,
        SUM(CASE WHEN size_type = 'small' THEN 1 ELSE 0 END) AS small_query_cnt,
        SUM(CASE WHEN size_type = 'medium' THEN 1 ELSE 0 END) AS medium_query_cnt,
        SUM(CASE WHEN size_type = 'large' THEN 1 ELSE 0 END) AS large_query_cnt,
        COUNT(*) AS tot_query_cnt
    FROM query_list_2
    GROUP BY start_hour
),
utilization_perc AS (
    SELECT 
        TRUNC(start_hour) AS sample_date,
        ROUND(100 * SUM(CASE WHEN tot_query_cnt > 0 THEN 1 ELSE 0 END) / 1440.0, 1) AS all_query_activity_perc,
        ROUND(100 * SUM(CASE WHEN small_query_cnt > 0 THEN 1 ELSE 0 END) / 1440.0, 1) AS small_query_activity_perc,
        ROUND(100 * SUM(CASE WHEN medium_query_cnt > 0 THEN 1 ELSE 0 END) / 1440.0, 1) AS medium_query_activity_perc,
        ROUND(100 * SUM(CASE WHEN large_query_cnt > 0 THEN 1 ELSE 0 END) / 1440.0, 1) AS large_query_activity_perc,
        MIN(start_hour) AS start_hour,
        MAX(start_hour) AS end_hour
    FROM hour_list_agg
    GROUP BY 1
),
activity_perc AS (
    SELECT 
        AVG(small_query_activity_perc) AS AVG_small_query_activity_perc, 
        AVG(medium_query_activity_perc) AS AVG_medium_query_activity_perc,
        AVG(large_query_activity_perc) AS AVG_large_query_activity_perc
    FROM utilization_perc
),
mincount AS (
    SELECT 
        TRUNC(start_hour) AS sample_date,
        SUM(CASE WHEN tot_query_cnt > 0 THEN 1 ELSE 0 END) AS tot_query_minute,
        SUM(CASE WHEN small_query_cnt > 0 THEN 1 ELSE 0 END) AS small_query_minute,
        SUM(CASE WHEN medium_query_cnt > 0 THEN 1 ELSE 0 END) AS medium_query_minute,
        SUM(CASE WHEN large_query_cnt > 0 THEN 1 ELSE 0 END) AS large_query_minute,
        MIN(start_hour) AS start_hour,
        MAX(start_hour) AS end_hour
    FROM hour_list_agg
    GROUP BY 1
),
avgmincount AS (
    SELECT 
        AVG(small_query_minute) AS avg_small_query_minute, 
        AVG(medium_query_minute) AS avg_medium_query_minute, 
        AVG(large_query_minute) AS avg_large_query_minute
    FROM mincount
),
final_output AS (
    SELECT * FROM activity_perc, avgmincount, workload_exec_seconds, 
    (SELECT COUNT(*) AS total_query_cnt FROM query_list) d
)
SELECT * FROM final_output;
