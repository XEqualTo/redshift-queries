-- This query retrieves metadata, state, and refresh status of materialized views (MVs) in Amazon Redshift.
-- It consists of three Common Table Expressions (CTEs):
-- 1. mv_info: Extracts basic information about MVs from 'stv_mv_info', including name, schema, state, and auto-refresh settings.
-- 2. mv_state: Retrieves the latest state change event for each MV from 'stl_mv_state'.
-- 3. mv_ref_status: Retrieves the most recent refresh status of each MV from 'svl_mv_refresh_status'.
-- Finally, the query joins these CTEs to present a comprehensive view of each materialized view's metadata, state, and refresh details.

WITH mv_info AS
(
  SELECT trim(db_name) AS dbname,
         trim("schema") AS namespace, 
         trim(name) AS mview_name,
         is_stale,
         state,
         CASE state
           WHEN 0 THEN 'The MV is fully recomputed when refreshed'
           WHEN 1 THEN 'The MV is incremental'
           WHEN 101 THEN 'The MV cant be refreshed due to a dropped column. This constraint applies even if the column isnt used in the MV'
           WHEN 102 THEN 'The MV cant be refreshed due to a changed column type. This constraint applies even if the column isnt used in the MV'
           WHEN 103 THEN 'The MV cant be refreshed due to a renamed table'
           WHEN 104 THEN 'The MV cant be refreshed due to a renamed column. This constraint applies even if the column isnt used in the MV'
           WHEN 105 THEN 'The MV cant be refreshed due to a renamed schema'
           ELSE NULL
         END AS state_desc,
         autorewrite,
         autorefresh
  FROM stv_mv_info
),
mv_state AS
(
  SELECT dbname,
         namespace,
         mview_name,
         state AS mv_state,
         event_desc,
         starttime AS event_starttime
  FROM (SELECT trim(db_name) AS dbname,
               trim(mv_schema) AS namespace, 
               trim(mv_name) AS mview_name,
               ROW_NUMBER() OVER (PARTITION BY db_name, mv_schema, mv_name ORDER BY starttime DESC) AS rnum,
               state,
               event_desc,
               starttime
        FROM stl_mv_state)
  WHERE rnum = 1
),
mv_ref_status AS
(
  SELECT dbname,
         refresh_db_username,
         namespace,
         mview_name,
         status AS refresh_status,
         refresh_type,
         starttime AS refresh_starttime,
         endtime AS refresh_endtime,
         datediff(ms, starttime, endtime)*1.0/1000 AS refresh_duration_secs
  FROM (SELECT trim(r.db_name) AS dbname,
               trim(r.schema_name) AS namespace, 
               pu.usename as refresh_db_username,
               trim(r.mv_name) AS mview_name,
               ROW_NUMBER() OVER (PARTITION BY r.db_name, r.schema_name, r.userid, r.mv_name ORDER BY starttime DESC) AS rnum,
               r.status,
               r.refresh_type,
               r.starttime,
               r.endtime
        FROM svl_mv_refresh_status r
        INNER JOIN pg_user pu on (r.userid = pu.usesysid))
  WHERE rnum = 1
)
SELECT i.*, 
       s.mv_state, s.event_desc, s.event_starttime,
       r.refresh_db_username, r.refresh_status, r.refresh_type, r.refresh_starttime, r.refresh_endtime, r.refresh_duration_secs
FROM mv_info i
  LEFT JOIN mv_state s ON (i.dbname = s.dbname AND i.namespace = s.namespace AND i.mview_name = s.mview_name)
  LEFT JOIN mv_ref_status r ON (i.dbname = r.dbname AND i.namespace = r.namespace AND i.mview_name = r.mview_name);
