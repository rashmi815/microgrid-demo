p/*=================================================================================================
 *         APPLYING K-MEANS CLUSTERING
 *
 * - Cluster periodogram feature vectors using K-means algorithm in MADlib using k=...
 *     - Find the centroids of the clusters, cluster assignments for the periodograms, and the
 *      distances between each point and centroid for each cluster
 *    - Calculate silhouette coefficients for every data point(pgram) in each cluster level.
 *     - Calculate the average silhouette coefficient for each cluster level.
 * - Re-cluster ...
 *
 *
 *=================================================================================================
 */

-- From the first round of clustering k=7 seems best based on the SSE vs. k plot
-- However, since this still leaves one very large cluster it will be useful to re-cluster that large cluster
-- Counts of data points in clusters for k=7:
SELECT * FROM mgdemo.mgdata_cluster_counts_tbl
WHERE k=7
ORDER BY cluster_id;
-- k;cluster_id;ct_bgid
-- 7;0;429
-- 7;1;1
-- 7;2;2
-- 7;3;1
-- 7;4;1
-- 7;5;1
-- 7;6;7


-- Pull out just those data points that are in k=7, cluster_id = 0
CREATE TABLE mgdemo.mgdata_pgram_array_round02_tbl AS
  SELECT *, row_number() over (ORDER BY bgid) AS bgid_r2
  FROM mgdemo.mgdata_pgram_array_cluster_id_tbl
  WHERE k=7 AND cluster_id=0
DISTRIBUTED BY (bgid_r2);
-- Query returned successfully: 429 rows affected, 13610 ms execution time.


-- Run the function for 3 to 15 clusters
SELECT mgdemo.run_kmeans_fn('mgdemo.mgdata_pgram_array_round02_tbl','mgdemo.mgdata_kmeans_output_round02_tbl',array_agg(km order by km))
FROM (SELECT generate_series(3,15,1) as km) t1;
-- INFO:  ===== query =====
-- INFO:  INSERT INTO mgdemo.mgdata_kmeans_output_round02_tbl SELECT 14, * FROM madlib.kmeanspp( ' mgdemo.mgdata_pgram_array_round02_tbl', 'pgram', 14, 'madlib.dist_norm2', 'madlib.avg', 100, 0.001);
-- INFO:  ===== i = 12 ==== k = 14 ===== END =====
-- INFO:  ===== i = 13 ==== k = 15 ===== START =====
-- INFO:  ===== query =====
-- INFO:  INSERT INTO mgdemo.mgdata_kmeans_output_round02_tbl SELECT 15, * FROM madlib.kmeanspp( ' mgdemo.mgdata_pgram_array_round02_tbl', 'pgram', 15, 'madlib.dist_norm2', 'madlib.avg', 100, 0.001);
-- INFO:  ===== i = 13 ==== k = 15 ===== END =====
-- Total query runtime: 52597 ms.
-- 1 row retrieved.


/*
 * TABLE of centroids periodograms unnest from k-means output unnested. This TABLE is
 * used unnest the two dimensional centroids array from k-means output.
 */
DROP TABLE IF EXISTS mgdemo.mgdata_km_centroids_unnest_full_round02_tbl;
CREATE TABLE mgdemo.mgdata_km_centroids_unnest_full_round02_tbl AS
  SELECT
    k,
    (index_id+(dim2-1))/dim2 AS array_id,
    index_id,
    centroid_points
  FROM
  (
    SELECT
      k,
      dim1,
      dim2,
      generate_series(1,dim1*dim2,1) as index_id,
      unnest(centroids) AS centroid_points
    FROM (
      SELECT k, centroids, array_upper(centroids,1) AS dim1, array_upper(centroids,2) AS dim2
      FROM mgdemo.mgdata_kmeans_output_round02_tbl
    ) t1
  ) t2
DISTRIBUTED BY (k,array_id,index_id);
-- Query returned successfully: 84240 rows affected, 825657 ms execution time.



-- TABLE of centroids AND respective cid's.
DROP TABLE IF EXISTS mgdemo.mgdata_km_centroids_unnest_onelevel_cluster_id_round02_tbl;
CREATE TABLE mgdemo.mgdata_km_centroids_unnest_onelevel_cluster_id_round02_tbl AS
  SELECT
    k,
    centroid_array,
    (madlib.closest_column(centroids_multidim_array, centroid_array)).column_id AS cluster_id
  FROM (
    SELECT
      t1.k,
      centroid_array,
      centroids as centroids_multidim_array
    FROM (
      SELECT
        k,
        array_agg(centroid_points ORDER BY index_id) AS centroid_array
      FROM
        mgdemo.mgdata_km_centroids_unnest_full_round02_tbl
      GROUP BY
        k, array_id
    ) t1,
    mgdemo.mgdata_kmeans_output_round02_tbl t2
    WHERE t1.k = t2.k
  ) t2
DISTRIBUTED RANDOMLY;
-- Query returned successfully: 117 rows affected, 2157 ms execution time.


-- Assign cluster IDs to all data points
DROP TABLE IF EXISTS mgdemo.mgdata_pgram_array_cluster_id_round02_tbl;
CREATE TABLE mgdemo.mgdata_pgram_array_cluster_id_round02_tbl AS
  SELECT
    t1.k as k_r2,
    bgid,
    bgid_r2,
    pgram,
    (madlib.closest_column(centroids_multidim_array, pgram)).column_id AS cluster_id
  FROM
    mgdemo.mgdata_pgram_array_round02_tbl,
    (SELECT k, centroids AS centroids_multidim_array FROM mgdemo.mgdata_kmeans_output_round02_tbl) t1
DISTRIBUTED BY (k_r2,cluster_id,bgid_r2);
-- Query returned successfully: 5577 rows affected, 3156 ms execution time.
