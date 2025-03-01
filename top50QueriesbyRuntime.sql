-- This query retrieves query execution metrics from Amazon Redshift system tables.
-- It gathers statistics on query performance, disk spill usage, compile times, 
-- and execution trends for the past 7 days. It also identifies alerts related to 
-- inefficient query patterns and summarizes key insights on executed queries.

SELECT TRIM(dbname) AS dbname,
       TRIM(db_username) AS db_username,
       MAX(SUBSTRING(replace(qrytext,chr (34),chr (92) + chr (34)),1,500)) AS qrytext,
       COUNT(query) AS num_queries,
       MIN(run_minutes) AS min_minutes,
       MAX(run_minutes) AS max_minutes,
       AVG(run_minutes) AS avg_minutes,
       SUM(run_minutes) AS total_minutes,
       SUM(compile_minutes) AS total_compile_minutes,
       SUM(num_compile_segments) AS total_num_compile_segments,
       MIN(query_temp_blocks_to_disk_mb) AS min_disk_spill_mb,
       MAX(query_temp_blocks_to_disk_mb) AS max_disk_spill_mb,
       AVG(query_temp_blocks_to_disk_mb) AS avg_disk_spill_mb,
       SUM(query_temp_blocks_to_disk_mb) AS total_disk_spill_mb,
       MAX(query) AS max_query_id,
       MAX(starttime)::DATE AS last_run,
       COUNT(DISTINCT starttime::DATE) AS num_days_executed,
       SUM(aborted) AS total_aborted,
       MAX(mylabel) qry_label,
       AVG(spectrum_object_count) AS avg_spectrum_object_used,
       AVG(federated_object_count) AS avg_federated_object_used,
       user_table_involved,
       TRIM(DECODE (event & 1,1,'Sortkey ','') || DECODE (event & 2,2,'Deletes ','') || DECODE (event & 4,4,'NL ','') || DECODE (event & 8,8,'Dist ','') || DECODE (event & 16,16,'Broacast ','') || DECODE (event & 32,32,'Stats ','')) AS Alert
FROM (SELECT stl_query.userid,
             pu.usename as db_username,
             label,
             stl_query.query,
             TRIM("DATABASE") AS dbname,
             NVL(qrytext_cur.text,TRIM(querytxt)) AS qrytext,
             MD5(NVL (qrytext_cur.text,TRIM(querytxt))) AS qry_md5,
             starttime,
             endtime,
             DATEDIFF(seconds,starttime,endtime)::NUMERIC(12,2) / 60 AS run_minutes,
             aborted,
             event,
             stl_query.label AS mylabel,
             CASE
               WHEN sqms.query_temp_blocks_to_disk IS NULL THEN 0
               ELSE sqms.query_temp_blocks_to_disk
             END AS query_temp_blocks_to_disk_mb,
             nvl(compile_secs,0)::NUMERIC(12,2) / 60 AS compile_minutes,
             nvl(num_compile_segments,0) AS num_compile_segments,
             s.user_table_involved,
             NVL(s3.spectrum_object_count,0) AS spectrum_object_count,
             NVL(f.federated_object_count,0) AS federated_object_count
      FROM stl_query
	  INNER JOIN pg_catalog.pg_user pu on (stl_query.userid = pu.usesysid)
        LEFT OUTER JOIN (SELECT query,
                                SUM(DECODE (TRIM(SPLIT_PART (event,':',1)),'Very selective query filter',1,'Scanned a large number of deleted rows',2,'Nested Loop Join in the query plan',4,'Distributed a large number of rows across the network',8,'Broadcasted a large number of rows across the network',16,'Missing query planner statistics',32,0)) AS event
                         FROM stl_alert_event_log
                         WHERE event_time > DATEADD(DAY,-7,CURRENT_DATE)
                         GROUP BY query) AS alrt ON alrt.query = stl_query.query
        LEFT OUTER JOIN (SELECT ut.xid,
                                TRIM(SUBSTRING(text FROM STRPOS (UPPER(text),'SELECT'))) AS TEXT
                         FROM stl_utilitytext ut
                         WHERE SEQUENCE = 0
                         AND   text ilike 'DECLARE%'
                         GROUP BY text,
                                  ut.xid) qrytext_cur ON (stl_query.xid = qrytext_cur.xid)
        LEFT OUTER JOIN svl_query_metrics_summary sqms
                     ON (sqms.userid = stl_query.userid
                    AND sqms.query = stl_query.query)
        LEFT OUTER JOIN (SELECT userid,
                                xid,
                                pid,
                                query,
                                MAX(datediff (ms,starttime,endtime)*1.0 / 1000) AS compile_secs,
                                SUM(compile) AS num_compile_segments
                         FROM svcs_compile
                         GROUP BY userid,
                                  xid,
                                  pid,
                                  query) c
                     ON (c.userid = stl_query.userid
                    AND c.xid = stl_query.xid
                    AND c.pid = stl_query.pid
                    AND c.query = stl_query.query)
        LEFT OUTER JOIN (SELECT s.userid,
                                s.query,
                                LISTAGG(DISTINCT TRIM(s.perm_table_name),', ') AS user_table_involved
                         FROM stl_scan s
                             INNER JOIN (SELECT DISTINCT(stv_tbl_perm.id) AS table_id
                    ,TRIM(pg_database.datname) AS database_name
                    ,TRIM(pg_namespace.nspname) AS schema_name
                    ,TRIM(relname) AS table_name
                FROM stv_tbl_perm
              INNER JOIN pg_database on pg_database.oid = stv_tbl_perm.db_id
              INNER JOIN pg_class on pg_class.oid = stv_tbl_perm.id
              INNER JOIN pg_namespace on pg_namespace.oid = pg_class.relnamespace
               WHERE schema_name NOT IN ('pg_internal', 'pg_catalog','pg_automv')) t ON (s.tbl = t.table_id)
                         WHERE s.perm_table_name NOT IN ('Internal Worktable','S3')
                         AND   s.perm_table_name NOT LIKE ('volt_tt%')
                         AND   t.schema_name NOT IN ('pg_internal','pg_catalog')
                         GROUP BY s.userid,
                                  s.query) s
                     ON (stl_query.userid = s.userid
                    AND stl_query.query = s.query)
        LEFT OUTER JOIN (SELECT s.userid,
                                s.query,
                                COUNT(1) AS spectrum_object_count
                         FROM svl_s3query_summary s
                         WHERE s.external_table_name NOT IN ('PG Subquery')
                         GROUP BY s.userid,
                                  s.query) s3
                     ON (stl_query.userid = s3.userid
                    AND stl_query.query = s3.query)
        LEFT OUTER JOIN (SELECT f.userid,
                                f.query,
                                COUNT(1) AS federated_object_count
                         FROM svl_federated_query f
                         GROUP BY f.userid,
                                  f.query) f
                     ON (stl_query.userid = f.userid
                    AND stl_query.query = f.query)
      WHERE stl_query.userid <> 1
      AND   NVL(qrytext_cur.text,TRIM(querytxt)) NOT LIKE 'padb_fetch_sample:%'
      AND   NVL(qrytext_cur.text,TRIM(querytxt)) NOT LIKE 'CREATE TEMP TABLE volt_tt_%'
      AND   stl_query.starttime >= DATEADD(DAY,-7,CURRENT_DATE))
GROUP BY TRIM(dbname),
         TRIM(db_username),
         qry_md5,
         user_table_involved,
         event
ORDER BY avg_minutes DESC, num_queries DESC LIMIT 50;
