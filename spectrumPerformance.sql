-- This query analyzes Redshift Spectrum performance by collecting metrics on external table queries,
-- including partition pruning efficiency, query parallelism, file size distribution, and execution time.
-- It retrieves data from system tables such as `svl_s3query_summary`, `stl_query`, `svv_external_tables`,
-- and `svv_external_partitions` over the past 7 days, grouping and sorting results by execution time.

SELECT trim(et.schemaname) AS namespace,
       trim(et.tablename) AS external_table_name,
       trim(lq.file_format) AS file_format,
       trim(l.s3_bucket) AS s3_bucket,
       CASE
         WHEN et.compressed = 1 THEN 'Y'
         ELSE 'N'
       END AS is_table_compressed,
       CASE
         WHEN lq.is_partitioned = 't' THEN 'Y'
         ELSE 'N'
       END AS is_table_partitioned,
       CASE
         WHEN l.avg_file_splits > 1 THEN 'Y'
         ELSE 'N'
       END AS is_file_spittable,
       MAX(nvl (ep.external_table_partition_count,0)) AS external_table_partition_count,
       COUNT(1) total_query_count,
       SUM(CASE WHEN lp.qualified_partitions < ep.external_table_partition_count THEN 1 ELSE 0 END) total_query_using_Partition_Pruning_count,
       ROUND(SUM(CASE WHEN lp.qualified_partitions < ep.external_table_partition_count THEN 1 ELSE 0 END) / COUNT(1)::NUMERIC(38,4)*100.0,4) AS pct_of_query_using_Partition_Pruning,
       nvl(AVG(CASE WHEN lp.qualified_partitions != 0 THEN lp.qualified_partitions END),0) AS avg_Qualified_Partitions,
       nvl(AVG(CASE WHEN lp.qualified_partitions != 0 THEN lp.avg_assigned_partitions END),0) AS avg_Assigned_Partitions,
       ROUND(nvl(AVG(CASE WHEN lp.qualified_partitions != 0 THEN lq.avg_request_parallelism END),0),4) AS avg_Parallelism,
       nvl(AVG(CASE WHEN lp.qualified_partitions != 0 THEN lq.files END),0) AS avg_Files,
       AVG(lq.splits) AS avg_Split,
       ROUND(AVG(l.avg_max_file_size_mb),4) AS avg_max_file_size_mb,
       ROUND(AVG(l.avg_file_size_mb),4) AS avg_file_size_mb,
       ROUND(AVG(elapsed / 1000000::NUMERIC(38,4)),4) avg_Elapsed_sec,
       ROUND(SUM(elapsed / 1000000::NUMERIC(38,4)),4) Total_Elapsed_sec,
       SUM(nvl(r.spectrum_scan_error_count,0)) AS total_spectrum_scan_error_count,
       AVG(nvl(r.spectrum_scan_error_count,0)) AS avg_spectrum_scan_error_count,
       SUM(CASE WHEN lp.qualified_partitions = 0 THEN 1 ELSE 0 END) Queries_Using_No_S3Files
FROM svl_s3query_summary lq
  INNER JOIN stl_query q
          ON (q.userid = lq.userid
         AND q.query = lq.query
         AND q.xid = lq.xid
         AND q.pid = lq.pid)
  INNER JOIN svv_external_tables et ON (q.database || '_' || et.schemaname || '_' || et.tablename = replace (replace (lq.external_table_name,'S3 Scan ',''),'S3 Subquery ','')) 
  LEFT OUTER JOIN (SELECT schemaname,
                          tablename,
                          COUNT(1) AS external_table_partition_count
                   FROM svv_external_partitions
                   GROUP BY schemaname,
                            tablename) ep
               ON (ep.schemaname = et.schemaname
              AND ep.tablename = et.tablename)
  LEFT OUTER JOIN svl_s3partition_summary lp ON lq.query = lp.query
  LEFT OUTER JOIN (SELECT query,
                          bucket AS s3_bucket,
                          AVG(max_file_size / 1000000.0) AS avg_max_file_size_mb,
                          AVG(avg_file_size / 1000000.0) AS avg_file_size_mb,
                          AVG(generated_splits) AS avg_file_splits
                   FROM svl_s3list
                   GROUP BY query,
                            bucket) l ON (lq.query = l.query)
  LEFT OUTER JOIN (SELECT query,
                          userid,
                          count(1) AS spectrum_scan_error_count
                   FROM svl_spectrum_scan_error
                   GROUP BY query, userid) r ON (lq.query = r.query and lq.userid = r.userid)
WHERE lq.starttime >= dateadd(day,-7,CURRENT_DATE)
AND   lq.aborted = 0
GROUP BY 1,2,3,4,5,6,7
ORDER BY Total_Elapsed_sec DESC LIMIT 50;
