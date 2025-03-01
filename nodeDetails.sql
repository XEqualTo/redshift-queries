-- This query retrieves Amazon Redshift node-level storage utilization details  
-- and identifies the node type based on storage capacity, NVMe status, and partition count.  
-- It performs the following steps:  
-- 1. Counts the number of slices per node (`node_slice` CTE).  
-- 2. Computes storage utilization percentage and capacity per node (`node_storage_utilization` CTE).  
-- 3. Identifies the node type by checking capacity, NVMe, and partition count.  
-- 4. Joins the results to get a comprehensive view of each node's storage and performance metrics.  
-- 5. Orders the final results by node ID.  

WITH node_slice AS (
  SELECT node,
         COUNT(1) AS slice_count
  FROM stv_slices
  GROUP BY node
),
node_storage_utilization AS (
  SELECT node::text AS node,
         (1.0 * used / capacity)::NUMERIC(8,4) * 100 AS storage_utilization_pct,
         1.0 * capacity / 1000 AS storage_capacity_gb,
         1.0 * used / 1000 AS storage_used_gb
  FROM stv_node_storage_capacity
)
SELECT 
  CASE
    WHEN capacity = 190633 AND NOT is_nvme THEN 'dc1.large'
    WHEN capacity = 380319 THEN 'dc1.8xlarge'
    WHEN capacity = 190633 AND is_nvme THEN 'dc2.large'
    WHEN capacity = 760956 THEN 'dc2.8xlarge'
    WHEN capacity = 726296 THEN 'dc2.8xlarge'
    WHEN capacity = 952455 THEN 'ds2.xlarge'
    WHEN capacity = 945026 THEN 'ds2.8xlarge'
    WHEN capacity = 954367 AND part_count = 1 THEN 'ra3.xlplus'
    WHEN capacity = 3339176 AND part_count = 1 THEN 'ra3.4xlarge'
    WHEN capacity = 3339176 AND part_count = 4 THEN 'ra3.16xlarge'
    ELSE 'unknown'
  END AS node_type,
  s.node,
  slice_count,
  storage_utilization_pct,
  storage_capacity_gb,
  storage_used_gb
FROM (
  SELECT p.host AS node,
         p.capacity,
         p.mount LIKE '/dev/nvme%' AS is_nvme,
         COUNT(1) AS part_count
  FROM stv_partitions p
  WHERE p.host = p.owner
  GROUP BY 1, 2, 3
) AS s
INNER JOIN node_slice n ON s.node = n.node
INNER JOIN node_storage_utilization ns ON s.node = ns.node
ORDER BY 2;
