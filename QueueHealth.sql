-- This query analyzes Redshift query workloads by categorizing them based on service class, queue type, and execution details.
-- It extracts various query attributes such as execution time, memory usage, concurrency scaling status, and query type.
-- The final output provides aggregated statistics like total queries executed, completed queries, and user-aborted queries,
-- grouped by execution hour, service class category, queue name, and database.

WITH workload AS (
    SELECT 
        TRIM(sq."database") AS dbname,
        CASE 
            WHEN sq.concurrency_scaling_status = 1 THEN 'burst'	        
            ELSE 'main' 
        END AS concurrency_scaling_status,
        CASE 
            WHEN sl.source_query IS NOT NULL THEN 'result_cache'       
            ELSE RTRIM(swsc.name) 
        END AS queue_name,
        swq.service_class,
        CASE
            WHEN swq.service_class BETWEEN 1 AND 4 THEN 'System'
            WHEN swq.service_class = 5 THEN 'Superuser'
            WHEN swq.service_class BETWEEN 6 AND 13 THEN 'Manual WLM queues'
            WHEN swq.service_class = 14 THEN 'SQA'
            WHEN swq.service_class = 15 THEN 'Redshift Maintenance'
            WHEN swq.service_class BETWEEN 100 AND 107 THEN 'Auto WLM'
        END AS service_class_category,	  
        sq.query AS query_id,
        CASE 
            WHEN REGEXP_INSTR(sq.querytxt, '(padb_|pg_internal)') THEN 'OTHER'
            WHEN REGEXP_INSTR(sq.querytxt, '([uU][nN][dD][oO][iI][nN][gG]) ') THEN 'SYSTEM'
            WHEN REGEXP_INSTR(sq.querytxt, '([aA][uU][tT][oO][mM][vV])') THEN 'AUTOMV'
            WHEN REGEXP_INSTR(sq.querytxt, '[uU][nN][lL][oO][aA][dD]') THEN 'UNLOAD'
            WHEN REGEXP_INSTR(sq.querytxt, '[cC][uU][rR][sS][oO][rR] ') THEN 'CURSOR'
            WHEN REGEXP_INSTR(sq.querytxt, '[fF][eE][tT][cC][hH] ') THEN 'CURSOR'
            WHEN REGEXP_INSTR(sq.querytxt, '[cC][rR][eE][aA][tT][eE] ') THEN 'CTAS'
            WHEN REGEXP_INSTR(sq.querytxt, '[dD][eE][lL][eE][tT][eE] ') THEN 'DELETE'
            WHEN REGEXP_INSTR(sq.querytxt, '[uU][pP][dD][aA][tT][eE] ') THEN 'UPDATE'
            WHEN REGEXP_INSTR(sq.querytxt, '[iI][nN][sS][eE][rR][tT] ') THEN 'INSERT'
            WHEN REGEXP_INSTR(sq.querytxt, '[vV][aA][cC][uU][uU][mM][ :]') THEN 'VACUUM'
            WHEN REGEXP_INSTR(sq.querytxt, '[aA][nN][aA][lL][yY][zZ][eE] ') THEN 'ANALYZE'		 
            WHEN REGEXP_INSTR(sq.querytxt, '[sS][eE][lL][eE][cC][tT] ') THEN 'SELECT'
            WHEN REGEXP_INSTR(sq.querytxt, '[cC][oO][pP][yY] ') THEN 'COPY'
            ELSE 'OTHER' 
        END AS query_type,
        DATE_TRUNC('hour', sq.starttime) AS workload_exec_hour,
        NVL(swq.est_peak_mem / 1024.0 / 1024.0 / 1024.0, 0.0) AS est_peak_mem_gb,
        DECODE(swq.final_state, 'Completed', DECODE(swr.action, 'abort', 0, DECODE(sq.aborted, 0, 1, 0)), 'Evicted', 0, NULL, DECODE(sq.aborted, 0, 1, 0)::INT) AS is_completed,
        DECODE(swq.final_state, 'Completed', DECODE(swr.action, 'abort', 1, 0), 'Evicted', 1, NULL, 0) AS is_evicted_aborted,
        DECODE(swq.final_state, 'Completed', DECODE(swr.action, 'abort', 0, DECODE(sq.aborted, 1, 1, 0)), 'Evicted', 0, NULL, DECODE(sq.aborted, 1, 1, 0)::INT) AS is_user_aborted,
        CASE WHEN sl.from_sp_call IS NOT NULL THEN 1 ELSE 0 END AS from_sp_call,
        CASE WHEN alrt.num_events IS NULL THEN 0 ELSE alrt.num_events END AS alerts,
        CASE WHEN dsk.num_diskbased > 0 THEN 1 ELSE 0 END AS is_query_diskbased,
        NVL(c.num_compile_segments, 0) AS num_compile_segments,
        CAST(CASE WHEN sqms.query_queue_time IS NULL THEN 0 ELSE sqms.query_queue_time END AS DECIMAL(26,6)) AS query_queue_time_secs,
        NVL(c.max_compile_time_secs, 0) AS max_compile_time_secs,
        sl.starttime,
        sl.endtime,
        sl.elapsed,
        CAST(sl.elapsed * 0.000001 AS DECIMAL(26,6)) AS query_execution_time_secs,
        sl.elapsed * 0.000001 - NVL(c.max_compile_time_secs, 0) - NVL(sqms.query_queue_time, 0) AS actual_execution_time_secs,
        CASE WHEN sqms.query_temp_blocks_to_disk IS NULL THEN 0 ELSE sqms.query_temp_blocks_to_disk END AS query_temp_blocks_to_disk_mb,
        CAST(CASE WHEN sqms.query_cpu_time IS NULL THEN 0 ELSE sqms.query_cpu_time END AS DECIMAL(26,6)) AS query_cpu_time_secs,
        NVL(sqms.scan_row_count, 0) AS scan_row_count,
        NVL(sqms.return_row_count, 0) AS return_row_count,
        NVL(sqms.nested_loop_join_row_count, 0) AS nested_loop_join_row_count,
        NVL(uc.usage_limit_count, 0) AS cs_usage_limit_count
    FROM stl_query sq
    INNER JOIN svl_qlog sl ON (sl.userid = sq.userid AND sl.query = sq.query)
    LEFT OUTER JOIN svl_query_metrics_summary sqms ON (sqms.userid = sq.userid AND sqms.query = sq.query)					
    LEFT OUTER JOIN stl_wlm_query swq ON (sq.userid = swq.userid AND sq.query = swq.query)
    LEFT OUTER JOIN stl_wlm_rule_action swr ON (sq.userid = swr.userid AND sq.query = swr.query AND swq.service_class = swr.service_class)
    LEFT OUTER JOIN stv_wlm_service_class_config swsc ON (swsc.service_class = swq.service_class)
    LEFT OUTER JOIN (
        SELECT sae.query, CAST(1 AS INTEGER) AS num_events FROM svcs_alert_event_log sae GROUP BY sae.query
    ) AS alrt ON (alrt.query = sq.query)  
    LEFT OUTER JOIN (
        SELECT sqs.userid, sqs.query, 1 AS num_diskbased FROM svcs_query_summary sqs WHERE sqs.is_diskbased = 't' GROUP BY sqs.userid, sqs.query
    ) AS dsk ON (dsk.userid = sq.userid AND dsk.query = sq.query)  
    LEFT OUTER JOIN (
        SELECT userid, xid, pid, query, MAX(DATEDIFF(ms, starttime, endtime) * 1.0 / 1000) AS max_compile_time_secs, SUM(compile) AS num_compile_segments
        FROM svcs_compile GROUP BY userid, xid, pid, query
    ) c ON (c.userid = sq.userid AND c.xid = sq.xid AND c.pid = sq.pid AND c.query = sq.query)                 
    LEFT OUTER JOIN (
        SELECT query, xid, pid, COUNT(1) AS usage_limit_count FROM stl_usage_control WHERE feature_type = 'CONCURRENCY_SCALING' GROUP BY query, xid, pid
    ) uc ON (uc.xid = sq.xid AND uc.pid = sq.pid AND uc.query = sq.query)                  	   
    WHERE sq.userid <> 1 
        AND sq.querytxt NOT LIKE 'padb_fetch_sample%'
        AND sq.starttime >= DATEADD(DAY, -7, CURRENT_DATE)
)
SELECT workload_exec_hour, service_class_category, service_class, queue_name, concurrency_scaling_status, dbname, query_type,
       SUM(is_completed) + SUM(is_user_aborted) + SUM(is_evicted_aborted) AS total_query_count,
       SUM(is_completed) AS completed_query_count,
       SUM(is_user_aborted) AS user_aborted_count
FROM workload
GROUP BY workload_exec_hour, service_class_category, service_class, queue_name, concurrency_scaling_status, dbname, query_type;
