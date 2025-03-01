-- This query analyzes workload patterns in an Amazon Redshift database 
-- by categorizing queries into different types (SELECT, INSERT, DELETE, DDL, etc.)
-- It aggregates query execution counts and durations by database, date, and hour.

WITH workload_profile AS (
  -- Extract query metadata, categorize queries based on keywords, 
  -- and calculate total execution count and duration.
  SELECT 
    trim(t."database") AS dbname,
    s.statement_type,
    CAST(CASE 
         WHEN regexp_instr (s.statement_text,'(padb_|pg_internal)') THEN 'SYSTEM' 
         WHEN regexp_instr (s.statement_text,'([uU][nN][dD][oO][iI][nN][gG]) ') THEN 'SYSTEM' 
         WHEN regexp_instr (s.statement_text,'([aA][uU][tT][oO][mM][vV])') THEN 'AUTOMV' 
         WHEN regexp_instr (s.statement_text,'[uU][nN][lL][oO][aA][dD]') THEN 'UNLOAD' 
         WHEN regexp_instr (s.statement_text,'[cC][uU][rR][sS][oO][rR] ') THEN 'CURSOR' 
         WHEN regexp_instr (s.statement_text,'[cC][rR][eE][aA][tT][eE] ') THEN 'DDL' 
         WHEN regexp_instr (s.statement_text,'[aA][lL][tT][eE][rR] ') THEN 'DDL' 
         WHEN regexp_instr (s.statement_text,'[dD][rR][oO][pP] ') THEN 'DDL' 
         WHEN regexp_instr (s.statement_text,'[tT][rR][uU][nN][cC][aA][tT][eE] ') THEN 'DDL' 
         WHEN regexp_instr (s.statement_text,'[cC][aA][lL][lL] ') THEN 'CALL_PROCEDURE' 
         WHEN regexp_instr (s.statement_text,'[gG][rR][aA][nN][tT] ') THEN 'DCL' 
         WHEN regexp_instr (s.statement_text,'[rR][eE][vV][oO][kK][eE] ') THEN 'DCL' 
         WHEN regexp_instr (s.statement_text,'[fF][eE][tT][cC][hH] ') THEN 'CURSOR' 
         WHEN regexp_instr (s.statement_text,'[dD][eE][lL][eE][tT][eE] ') THEN 'DELETE'
         WHEN regexp_instr (s.statement_text,'[uU][pP][dD][aA][tT][eE] ') THEN 'UPDATE' 
         WHEN regexp_instr (s.statement_text,'[iI][nN][sS][eE][rR][tT] ') THEN 'INSERT' 
         WHEN regexp_instr (s.statement_text,'[vV][aA][cC][uU][uU][mM][ :]') THEN 'VACUUM' 
         WHEN regexp_instr (s.statement_text,'[aA][nN][aA][lL][yY][zZ][eE] ') THEN 'ANALYZE' 
         WHEN regexp_instr (s.statement_text,'[sS][eE][lL][eE][cC][tT] ') THEN 'SELECT' 
         WHEN regexp_instr (s.statement_text,'[cC][oO][pP][yY] ') THEN 'COPY' 
         ELSE 'OTHER' 
         END AS VARCHAR(32)) AS query_type,
    DATEPART(HOUR, t.starttime) AS query_hour,
    t.starttime::DATE AS query_date,
    ROUND(SUM(duration_secs), 1) AS query_duration_secs,
    COUNT(*) AS query_count
  FROM (
    -- Retrieve statement metadata, filter for queries in the past 7 days
    SELECT 
      s.userid,
      s.type AS statement_type,
      s.xid,
      s.pid,
      DATEDIFF(milliseconds, starttime, endtime)::NUMERIC(38,4) / 1000 AS duration_secs,
      CAST(REPLACE(rtrim(s.text), '\\n', '') AS VARCHAR(800)) AS statement_text
    FROM svl_statementtext s
    WHERE s.sequence = 0
      AND s.starttime >= DATEADD(DAY, -7, CURRENT_DATE)
  ) s
  LEFT OUTER JOIN stl_query t
    ON (s.xid = t.xid AND s.pid = t.pid AND s.userid = t.userid)
  WHERE s.statement_text NOT LIKE '%volt_tt_%'
  GROUP BY 1,2,3,4,5
)

-- Aggregating query execution statistics by query type
SELECT 
  wp.dbname,
  wp.query_date,
  wp.query_hour,
  MAX(NVL(CASE WHEN wp.query_type = 'SELECT' THEN query_count ELSE 0 END, 0))::bigint AS select_count,
  MAX(NVL(CASE WHEN wp.query_type = 'SELECT' THEN query_duration_secs ELSE 0 END, 0)) AS select_duration_secs,
  MAX(NVL(CASE WHEN wp.query_type = 'CURSOR' THEN query_count ELSE 0 END, 0))::bigint AS cursor_count,
  MAX(NVL(CASE WHEN wp.query_type = 'CURSOR' THEN query_duration_secs ELSE 0 END, 0)) AS cursor_duration_secs,
  MAX(NVL(CASE WHEN wp.query_type = 'UNLOAD' THEN query_count ELSE 0 END, 0))::bigint AS unload_count,
  MAX(NVL(CASE WHEN wp.query_type = 'UNLOAD' THEN query_duration_secs ELSE 0 END, 0)) AS unload_duration_secs,
  MAX(NVL(CASE WHEN wp.query_type = 'COPY' THEN query_count ELSE 0 END, 0))::bigint AS copy_count,
  MAX(NVL(CASE WHEN wp.query_type = 'COPY' THEN query_duration_secs ELSE 0 END, 0)) AS copy_duration_secs,
  MAX(NVL(CASE WHEN wp.query_type = 'INSERT' THEN query_count ELSE 0 END, 0))::bigint AS insert_count,
  MAX(NVL(CASE WHEN wp.query_type = 'INSERT' THEN query_duration_secs ELSE 0 END, 0)) AS insert_duration_secs,
  MAX(NVL(CASE WHEN wp.query_type = 'UPDATE' THEN query_count ELSE 0 END, 0))::bigint AS update_count,
  MAX(NVL(CASE WHEN wp.query_type = 'UPDATE' THEN query_duration_secs ELSE 0 END, 0)) AS update_duration_secs,
  MAX(NVL(CASE WHEN wp.query_type = 'DELETE' THEN query_count ELSE 0 END, 0))::bigint AS delete_count,
  MAX(NVL(CASE WHEN wp.query_type = 'DELETE' THEN query_duration_secs ELSE 0 END, 0)) AS delete_duration_secs,
  MAX(NVL(CASE WHEN wp.query_type = 'VACUUM' THEN query_count ELSE 0 END, 0))::bigint AS vacuum_count,
  MAX(NVL(CASE WHEN wp.query_type = 'VACUUM' THEN query_duration_secs ELSE 0 END, 0)) AS vacuum_duration_secs,
  MAX(NVL(CASE WHEN wp.query_type = 'ANALYZE' THEN query_count ELSE 0 END, 0))::bigint AS analyze_count,
  MAX(NVL(CASE WHEN wp.query_type = 'ANALYZE' THEN query_duration_secs ELSE 0 END, 0)) AS analyze_duration_secs,
  MAX(NVL(CASE WHEN wp.query_type = 'CALL_PROCEDURE' THEN query_count ELSE 0 END, 0))::bigint AS call_proc_count,
  MAX(NVL(CASE WHEN wp.query_type = 'CALL_PROCEDURE' THEN query_duration_secs ELSE 0 END, 0)) AS call_proc_duration_secs,
  MAX(NVL(CASE WHEN wp.query_type = 'DDL' THEN query_count ELSE 0 END, 0))::bigint AS ddl_count,
  MAX(NVL(CASE WHEN wp.query_type = 'DDL' THEN query_duration_secs ELSE 0 END, 0)) AS ddl_duration_secs,
  MAX(NVL(CASE WHEN wp.query_type = 'DCL' THEN query_count ELSE 0 END, 0))::bigint AS dcl_count,
  MAX(NVL(CASE WHEN wp.query_type = 'DCL' THEN query_duration_secs ELSE 0 END, 0)) AS dcl_duration_secs,
  MAX(NVL(CASE WHEN wp.query_type = 'AUTOMV' THEN query_count ELSE 0 END, 0))::bigint AS automv_count,
  MAX(NVL(CASE WHEN wp.query_type = 'AUTOMV' THEN query_duration_secs ELSE 0 END, 0)) AS automv_duration_secs,
  MAX(NVL(CASE WHEN wp.query_type = 'OTHER' THEN query_count ELSE 0 END, 0))::bigint AS other_count,
  MAX(NVL(CASE WHEN wp.query_type = 'OTHER' THEN query_duration_secs ELSE 0 END, 0)) AS other_duration_secs
FROM workload_profile wp
WHERE wp.dbname <> 'padb_harvest'
GROUP BY 1,2,3
ORDER BY 1,2,3;
