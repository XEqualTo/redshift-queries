/*
This query analyzes Workload Management (WLM) and commit time performance in an Amazon Redshift cluster. 

It calculates the percentage of time spent in different phases of query execution:
1. `pct_wlm_queue_time`: Percentage of time spent in WLM queue.
2. `pct_exec_only_time`: Percentage of execution-only time.
3. `pct_commit_queue_time`: Percentage of time spent waiting in commit queue.
4. `pct_commit_time`: Percentage of total commit time.

The query fetches data from:
- `stl_query` (tracks query execution details)
- `stl_commit_stats` (commit statistics)
- `stl_wlm_query` (WLM query details)
- `stv_wlm_service_class_config` (WLM service class configurations)

It groups data by date, service class, queue name, and node.
*/

SELECT 
    IQ.*, 
    (IQ.wlm_queue_time_ms / IQ.wlm_start_commit_time_ms) * 100.0::NUMERIC(6,2) AS pct_wlm_queue_time,
    (IQ.exec_only_time_ms / IQ.wlm_start_commit_time_ms) * 100.0::NUMERIC(6,2) AS pct_exec_only_time,
    (IQ.commit_queue_time_ms / IQ.wlm_start_commit_time_ms) * 100.0::NUMERIC(6,2) AS pct_commit_queue_time,
    (IQ.commit_time_ms / IQ.wlm_start_commit_time_ms) * 100.0::NUMERIC(6,2) AS pct_commit_time
FROM 
(
    SELECT 
        TRUNC(b.starttime) AS day,
        d.service_class,
        RTRIM(s.name) AS queue_name,
        c.node,
        COUNT(DISTINCT c.xid) AS count_commit_xid,
        
        -- Total time from WLM start to commit completion in milliseconds
        SUM(DATEDIFF('microsec', d.service_class_start_time, c.endtime) * 0.001)::NUMERIC(38,4) AS wlm_start_commit_time_ms,
        
        -- Total WLM queue time in milliseconds
        SUM(DATEDIFF('microsec', d.queue_start_time, d.queue_end_time) * 0.001)::NUMERIC(38,4) AS wlm_queue_time_ms,
        
        -- Total execution time in milliseconds
        SUM(DATEDIFF('microsec', b.starttime, b.endtime) * 0.001)::NUMERIC(38,4) AS exec_only_time_ms,
        
        -- Total commit time in milliseconds
        SUM(DATEDIFF('microsec', c.startwork, c.endtime) * 0.001)::NUMERIC(38,4) AS commit_time_ms,
        
        -- Total commit queue time in milliseconds
        SUM(DATEDIFF('microsec', DECODE(c.startqueue, '2000-01-01 00:00:00', c.startwork, c.startqueue), c.startwork) * 0.001)::NUMERIC(38,4) AS commit_queue_time_ms
    FROM 
        stl_query b
        JOIN stl_commit_stats c ON b.xid = c.xid
        JOIN stl_wlm_query d ON b.query = d.query
        JOIN stv_wlm_service_class_config s ON d.service_class = s.service_class
    WHERE 
        c.xid > 0  -- Filter out non-transactional queries
    GROUP BY 
        1, 2, 3, 4
    ORDER BY 
        1, 2, 3, 4
) IQ;
