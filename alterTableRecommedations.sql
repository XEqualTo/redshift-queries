-- This query retrieves table optimization recommendations from Amazon Redshift.  
-- It joins the `svv_alter_table_recommendations` view with table metadata to filter out internal schemas.  
-- The query provides recommendations on column encoding, sort keys, and distribution styles  
-- along with details such as table name, schema, database, and whether the recommendation is automatically eligible.  

SELECT r.type,
       trim(t.database_name) AS dbname,
       t.schema_name AS namespace,
       r.table_id,
       t.table_name,
       r.group_id,
       r.ddl,
       r.auto_eligible
FROM svv_alter_table_recommendations r
  INNER JOIN (SELECT DISTINCT(stv_tbl_perm.id) AS table_id
                    ,TRIM(pg_database.datname) AS database_name
                    ,TRIM(pg_namespace.nspname) AS schema_name
                    ,TRIM(relname) AS table_name
                FROM stv_tbl_perm
              INNER JOIN pg_database on pg_database.oid = stv_tbl_perm.db_id
              INNER JOIN pg_class on pg_class.oid = stv_tbl_perm.id
              INNER JOIN pg_namespace on pg_namespace.oid = pg_class.relnamespace
               WHERE schema_name NOT IN ('pg_internal', 'pg_catalog','pg_automv')) t
          ON (r.database = t.database_name
         AND r.table_id = t.table_id);
