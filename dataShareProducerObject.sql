-- This query retrieves information about tables and materialized views 
-- shared by a producer in an Amazon Redshift datashare.
-- It joins metadata from various system tables to get details such as:
-- - Table row count
-- - Last vacuum date
-- - Insert and delete statistics
-- - Whether a materialized view is stale or performing full refreshes
-- The final result filters for outbound shared objects and orders them
-- by share name, namespace, object type, and producer account.

WITH shared_table AS
(
  SELECT t.schema_name|| '.' ||t.table_name AS object_name,
         MAX(t.table_rows) AS table_rows,
         MAX(v.eventtime) AS last_vacuum_date,
         MAX(CASE WHEN v.is_recluster = 0 THEN 'N' WHEN v.is_recluster = 1 THEN 'Y' ELSE NULL END) AS is_last_vacuum_recluster,
         MAX(i.num_insert_operation) AS num_insert_operation,
         MAX(i.total_inserted_rows) AS total_inserted_rows,
         MAX(i.last_insert_date) AS last_insert_date,
         MAX(d.num_delete_operation) AS num_delete_operation,
         MAX(d.total_deleted_rows) AS total_deleted_rows,
         MAX(d.last_delete_date) AS last_delete_date
  FROM (SELECT DISTINCT (stv_tbl_perm.id) table_id,
               TRIM(pg_database.datname) AS "database",
               TRIM(pg_namespace.nspname) AS schema_name,
               TRIM(relname) AS table_name,
               reltuples::bigint AS table_rows
        FROM stv_tbl_perm
          JOIN pg_database ON pg_database.oid = stv_tbl_perm.db_id
          JOIN pg_class ON pg_class.oid = stv_tbl_perm.id
          JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
        WHERE schema_name NOT IN ('pg_internal','pg_catalog','pg_automv')) t
    LEFT OUTER JOIN stl_vacuum v
                 ON (t.table_id = v.table_id
                AND v.status NOT LIKE 'Skip%')
    LEFT OUTER JOIN (SELECT tbl,
                            COUNT(1) AS num_insert_operation,
                            SUM(ROWS) AS total_inserted_rows,
                            MAX(endtime) AS last_insert_date
                     FROM stl_insert
                     GROUP BY 1) i ON (i.tbl = t.table_id)
    LEFT OUTER JOIN (SELECT tbl,
                            COUNT(1) AS num_delete_operation,
                            SUM(ROWS) AS total_deleted_rows,
                            MAX(endtime) AS last_delete_date
                     FROM stl_delete
                     GROUP BY 1) d ON (d.tbl = t.table_id)
  GROUP BY 1
)
SELECT d.share_type,
       d.share_name,
       CASE 
         WHEN d.include_new = true THEN 'True' 
         WHEN d.include_new = false THEN 'False' 
         ELSE NULL 
       END AS include_new,
       d.producer_account,
       d.object_type,
       REPLACE(SUBSTRING(d.object_name,1,STRPOS (d.object_name,'.')),'.','') AS namespace,
       SUBSTRING(d.object_name,STRPOS (d.object_name,'.') +1) AS object_name,
       ti.table_rows,
       ti.last_vacuum_date,
       ti.is_last_vacuum_recluster,
       ti.num_insert_operation,
       ti.total_inserted_rows,
       ti.last_insert_date,
       ti.num_delete_operation,
       ti.total_deleted_rows,
       ti.last_delete_date,
       CASE
         WHEN mi.is_stale = 't' THEN 'Y'
         WHEN mi.is_stale = 'f' THEN 'N'
         ELSE NULL
       END AS is_mv_stale,
       CASE
         WHEN mi.state = 1 THEN 'Y'
         WHEN mi.state <> 1 THEN 'N'
         ELSE NULL
       END AS is_mv_incremental_refresh,
       CASE
         WHEN mi.autorefresh = 1 THEN 'Y'
         WHEN mi.autorefresh <> 1 THEN 'N'
         ELSE NULL
       END AS is_mv_auto_refresh
FROM svv_datashare_objects d
  LEFT OUTER JOIN shared_table ti ON (d.object_name = ti.object_name)
  LEFT OUTER JOIN stv_mv_info mi ON (d.object_name = mi.schema|| '.' ||mi.name)
WHERE d.share_type = 'OUTBOUND'
ORDER BY 2,3,5,4;
