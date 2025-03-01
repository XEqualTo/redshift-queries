-- This query retrieves table alert details from Amazon Redshift's system logs.
-- It identifies tables with high distribution alerts, sort key alerts, missing statistics, and nested loops.

SELECT 
    TRIM(q."database") AS dbname,  -- Extracts the database name
    TRIM(s.perm_table_name) AS table_name,  -- Extracts the table name
    COALESCE(
        SUM(
            ABS(
                DATEDIFF(
                    microsecond,
                    COALESCE(b.starttime, d.starttime, s.starttime),
                    CASE 
                        WHEN COALESCE(b.endtime, d.endtime, s.endtime) > COALESCE(b.starttime, d.starttime, s.starttime) 
                        THEN COALESCE(b.endtime, d.endtime, s.endtime) 
                        ELSE COALESCE(b.starttime, d.starttime, s.starttime) 
                    END
                )
            )
        ) / 1000000::NUMERIC(38,4), 
        0
    ) AS alert_seconds, -- Total duration of alerts in seconds

    COALESCE(SUM(COALESCE(b.rows, d.rows, s.rows)), 0) AS alert_rowcount, -- Total number of rows affected
    TRIM(SPLIT_PART(l.event, ':', 1)) AS alert_event, -- Extracts the alert event type
    SUBSTRING(TRIM(l.solution), 1, 200) AS alert_solution, -- Provides suggested solutions for alerts
    MAX(l.query) AS alert_sample_query, -- Stores a sample query related to the alert
    COUNT(DISTINCT l.query) AS alert_querycount -- Counts unique queries that triggered alerts

FROM stl_alert_event_log AS l
LEFT JOIN stl_scan AS s
    ON s.query = l.query
    AND s.slice = l.slice
    AND s.segment = l.segment
    AND s.userid > 1
    AND s.perm_table_name NOT IN ('Internal Worktable', 'S3')
    AND s.perm_table_name NOT LIKE 'volt_tt%'
    AND s.perm_table_name NOT LIKE 'mv_tbl__auto_mv%'

LEFT JOIN stl_dist AS d
    ON d.query = l.query
    AND d.slice = l.slice
    AND d.segment = l.segment
    AND d.userid > 1

LEFT JOIN stl_bcast AS b
    ON b.query = l.query
    AND b.slice = l.slice
    AND b.segment = l.segment
    AND b.userid > 1    

LEFT JOIN stl_query AS q   
    ON q.query = l.query
    AND q.xid = l.xid
    AND q.userid > 1           

WHERE l.userid > 1
  AND TRIM(s.perm_table_name) IS NOT NULL
  AND l.event_time >= DATEADD(day, -7, CURRENT_DATE) -- Filters events from the past 7 days

GROUP BY 1, 2, 5, 6
ORDER BY alert_seconds DESC; -- Orders results by highest alert duration first
