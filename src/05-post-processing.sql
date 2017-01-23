/*==================================================================================
*         POST-PROCESSING
*
* - Calculate number of periodograms in each cluster.
* - Find cluster assignments for all periodograms.
* - Calculate mean and std deviation of distances in each cluster.
* - Calculate pairwise L2-norm distances between centroids.

*==================================================================================*/

-- TABLE of counts of points in clusters
CREATE TABLE mgdemo.mgdata_cluster_counts_tbl AS
  SELECT k, cluster_id, count(bgid) as ct_bgid
  FROM mgdemo.mgdata_cluster_id_tbl
  GROUP BY k, cluster_id
DISTRIBUTED RANDOMLY;
-- Query returned successfully: 322 rows affected, 146 ms execution time.

-- Check to make sure that the total number of bgid(s) is 442
-- (the number of data points that went in to clustering)
SELECT sum_ct_bgid, array_agg(k ORDER BY k) as array_k, COUNT(k) as ct_k
FROM (
  SELECT k, sum(ct_bgid) as sum_ct_bgid
  FROM mgdemo.mgdata_cluster_counts_tbl
  GROUP BY k
) t1
GROUP BY sum_ct_bgid
ORDER BY sum_ct_bgid;
-- Total query runtime: 47 ms.
-- 1 row retrieved.
-- 442;"{3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25}";23


-- TABLE of pairwise L2-norm distances between the centroids in each level.
CREATE TABLE asdemo.as_clust_centroids_level1to4_dist_t1 AS
  SELECT
    row_number() over () AS dist_id,
    cid_t1,
    cid_t2,
    sqrt(madlib.array_dot(pgram_diff, pgram_diff)) AS dist_l2norm,
  FROM (
    SELECT
      t1.k
      t1.cluster_id as cid_t1,
      t2.cluster_id as cid_t2,
      madlib.array_sub(t1.km_centroid,t2.km_centroid) AS pgram_diff
    FROM
      (SELECT a2.*, pgram FROM mgdemo.mgdata_pgram_array_tbl a1, mgdemo.mgdata_cluster_id_tbl a2 WHERE t1.bgid=t2.bgid) t1
      CROSS JOIN
      (SELECT b2.*, pgram FROM mgdemo.mgdata_pgram_array_tbl b1, mgdemo.mgdata_cluster_id_tbl b2 WHERE t4.bgid=t5.bgid) t2
    WHERE t1.cluster_id < t2.cluster_id
  ) t3
  DISTRIBUTED RANDOMLY;


-- TABLE of pairwise L2-norm distances between the centroids in each level.
CREATE TABLE asdemo.as_clust_centroids_level1to4_dist_t1 AS
  SELECT row_number() over () AS dist_id,
  unique_clust_level1to4_id_t1,
  unique_clust_level1to4_id_t2,
  cluster_level_t1,
  cluster_level_t2,
  cid_of_prev_level_t1,
  cid_of_prev_level_t2,
  cid_t1,
  cid_t2,
  sqrt(madlib.array_dot(pgram_diff, pgram_diff)) AS dist_l2norm,
  final_clust_flag_t1,
  final_clust_flag_t2
  FROM (
    SELECT t1.unique_clust_level1to4_id AS unique_clust_level1to4_id_t1,
      t2.unique_clust_level1to4_id AS unique_clust_level1to4_id_t2,
      t1.cluster_level AS cluster_level_t1,
      t2.cluster_level AS cluster_level_t2,
      t1.cid_of_prev_level AS cid_of_prev_level_t1,
      t2.cid_of_prev_level AS cid_of_prev_level_t2,
      t1.cid AS cid_t1,
      t2.cid AS cid_t2,
      t1.final_clust_flag AS final_clust_flag_t1,
      t2.final_clust_flag AS final_clust_flag_t2,
      madlib.array_sub(t1.km_centroid,t2.km_centroid) AS pgram_diff
    FROM asdemo.as_clust_level1to4 t1 cross join asdemo.as_clust_level1to4 t2
    WHERE t1.unique_clust_level1to4_id < t2.unique_clust_level1to4_id
  ) t3
  DISTRIBUTED BY (cid_t1);
-- Query returned successfully: 780 rows affected, 33343 ms execution time.

-- TABLE of pairwise L2-norm distances between the final 36 centroids.
CREATE TABLE asdemo.as_clust_centroids_final36_dist_t1 AS
  SELECT *
  FROM asdemo.as_clust_centroids_level1to4_dist_t1
  WHERE final_clust_flag_t1 = 1
  AND final_clust_flag_t2 = 1
  DISTRIBUTED BY (cluster_level_t1);
-- Query returned successfully with no result in 2541 ms.
