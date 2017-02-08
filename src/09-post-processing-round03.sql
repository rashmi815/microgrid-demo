/*==================================================================================
*         POST-PROCESSING
*
* - Calculate number of periodograms in each cluster.
* - Find cluster assignments for all periodograms.
* - Calculate mean and std deviation of distances in each cluster.
* - Calculate pairwise L2-norm distances between centroids.

*==================================================================================*/

-- TABLE of counts of points in clusters
DROP TABLE IF EXISTS mgdemo.mgdata_cluster_counts_round03_tbl;
CREATE TABLE mgdemo.mgdata_cluster_counts_round03_tbl AS
  SELECT k, cluster_id, count(bgid) as ct_bgid
  FROM mgdemo.mgdata_pgram_array_cluster_id_round03_tbl
  GROUP BY k, cluster_id
DISTRIBUTED RANDOMLY;
-- Query returned successfully: 117 rows affected, 331 ms execution time.


-- Check to make sure that the total number of bgid(s) is 362
-- (the number of data points that went in to clustering)
SELECT sum_ct_bgid, array_agg(k ORDER BY k) as array_k, COUNT(k) as ct_k
FROM (
  SELECT k, sum(ct_bgid) as sum_ct_bgid
  FROM mgdemo.mgdata_cluster_counts_round03_tbl
  GROUP BY k
) t1
GROUP BY sum_ct_bgid
ORDER BY sum_ct_bgid;
-- 362;"{3,4,5,6,7,8,9,10,11,12,13,14,15}";13


-- Compute silhouette coefficients for each k
SELECT mgdemo.calc_silhouette_coef_fn(
  'mgdemo.mgdata_pgram_array_round03_tbl',
  'pgram',
  'mgdemo.mgdata_kmeans_output_round03_tbl',
  'centroids',
  'madlib.dist_norm2',
  'mgdemo.mgdata_km_silhcoef_round03_tbl'
);
-- INFO:  ===== i = 13 ==== k = 15 ===== START =====
-- INFO:  ===== query =====
-- INFO:  SELECT madlib.simple_silhouette ( 'mgdemo.mgdata_pgram_array_round03_tbl', 'pgram', (SELECT centroids FROM mgdemo.mgdata_kmeans_output_round03_tbl WHERE k = 15), 'madlib.dist_norm2')
-- INFO:  ===== silhouette coefficeint = 0.476579568724436 =====
-- INFO:  ===== query =====
-- INFO:  INSERT INTO mgdemo.mgdata_km_silhcoef_round03_tbl SELECT 15 AS k, 0.476579568724436 AS silhouette_coef;
-- INFO:  ===== i = 13 ==== k = 15 ===== END =====
-- Total query runtime: 976 ms.
-- 1 row retrieved.



-- Compute SSE (Sum of Squared Errors) for each k
DROP TABLE if EXISTS mgdemo.mgdata_km_sse_round03_tbl;
CREATE TABLE mgdemo.mgdata_km_sse_round03_tbl AS
  SELECT
    k,
    sum(point_to_centroid_error) AS sse
  FROM
  (
    SELECT
      clust_id_tbl.k,
      clust_id_tbl.bgid,
      clust_id_tbl.bgid_r2,
      clust_id_tbl.bgid_r3,
      clust_id_tbl.cluster_id,
      madlib.array_sum(madlib.array_square(madlib.array_sub(pgram,centroid_array))) AS point_to_centroid_error
    FROM
      mgdemo.mgdata_pgram_array_cluster_id_round03_tbl AS clust_id_tbl,
      mgdemo.mgdata_km_centroids_unnest_onelevel_cluster_id_round03_tbl AS kmeans_cent_unnest_one_tbl
    WHERE
      clust_id_tbl.k = kmeans_cent_unnest_one_tbl.k
      AND clust_id_tbl.cluster_id = kmeans_cent_unnest_one_tbl.cluster_id
  ) t1
GROUP BY k
DISTRIBUTED RANDOMLY;
-- Query returned successfully: 13 rows affected, 465 ms execution time.


select *, sse/1e6 from mgdemo.mgdata_km_sse_round03_tbl
order by k;
/*
3;5804230.92506824;5.80423092506824
4;5762466.00420014;5.76246600420014
5;4508165.87938095;4.50816587938095
6;3771192.60397736;3.77119260397736
7;3023050.91611148;3.02305091611148
8;2769768.70393478;2.76976870393479
9;3668454.02383408;3.66845402383408
10;2478501.0224952;2.4785010224952
11;2399378.47471028;2.39937847471028
12;2250219.2983061;2.2502192983061
13;2247819.66639686;2.24781966639686
14;2143672.9160794;2.1436729160794
15;2048459.83513195;2.04845983513195
*/

-- After looking at the above SSE values (the elbow plot) and the cluster count distributions,
-- heuristically we choose k=5
SELECT * FROM mgdemo.mgdata_cluster_counts_round03_tbl
WHERE k in (7,8) ORDER BY k, cluster_id;
-- k;cluster_id;ct_bgid
-- 7;0;247
-- 7;1;75
-- 7;2;21
-- 7;3;1
-- 7;4;1
-- 7;5;16
-- 7;6;1
-- 8;0;14
-- 8;1;64
-- 8;2;216
-- 8;3;1
-- 8;4;26
-- 8;5;26
-- 8;6;14
-- 8;7;1


-- Take k=8 here itself
-- TABLE of distances between pgrams and respective cluster centroids
DROP TABLE IF EXISTS mgdemo.mgdata_pgram_array_centroid_dist_round03_fork8_tbl;
CREATE TABLE mgdemo.mgdata_pgram_array_centroid_dist_round03_fork8_tbl AS
  SELECT
    t1.k,
    t2.cluster_id,
    bgid,
    bgid_r2,
    bgid_r3,
    sqrt(madlib.array_dot(madlib.array_sub(km_cent,km_pnts),madlib.array_sub(km_cent,km_pnts))) AS l2dist
  FROM (
    SELECT k, cluster_id, centroid_array::float8[] AS km_cent
    FROM mgdemo.mgdata_km_centroids_unnest_onelevel_cluster_id_round03_tbl
    WHERE k=8
  ) t1,
  (
    SELECT k, cluster_id, bgid, bgid_r2, bgid_r3, pgram::float8[] AS km_pnts
    FROM mgdemo.mgdata_pgram_array_cluster_id_round03_tbl
    WHERE k=8
  ) t2
  WHERE
    t1.k = t2.k
    AND t1.cluster_id = t2.cluster_id
DISTRIBUTED BY (k,cluster_id,bgid_r3);
-- Query returned successfully: 362 rows affected, 409 ms execution time.


-- TABLE of meter readings AND respective cluster assignments.
DROP TABLE IF EXISTS mgdemo.mgdata_pgram_array_unnest_cluster_id_round03_fork8_tbl;
CREATE TABLE mgdemo.mgdata_pgram_array_unnest_cluster_id_round03_fork8_tbl AS
  SELECT
    k,
    cluster_id,
    bgid,
    bgid_r2,
    bgid_r3,
    unnest(ridarray) AS point_id,
    unnest(pgram) AS pgram_points
  FROM (
    SELECT
      k, cluster_id, bgid, bgid_r2, bgid_r3, pgram::float8[], ridarray
    FROM
      (
        SELECT * FROM mgdemo.mgdata_pgram_array_cluster_id_round03_tbl
        WHERE k=8
      ) t1,
      (
        SELECT array_agg(rid ORDER BY rid) AS ridarray
        FROM (
          SELECT generate_series(1,array_upper(pgram,1),1) AS rid
          FROM (
            SELECT pgram FROM mgdemo.mgdata_pgram_array_cluster_id_round03_tbl
            WHERE k=8 LIMIT 1
          ) t2
        ) t3
      ) t4
  ) t5
DISTRIBUTED BY (k,cluster_id,bgid,point_id);
-- Query returned successfully: 260640 rows affected, 1538 ms execution time.


DROP TABLE IF EXISTS mgdemo.mgdata_clean_with_id_and_cluster_id_round03_fork8_tbl;
CREATE TABLE mgdemo.mgdata_clean_with_id_and_cluster_id_round03_fork8_tbl AS
  SELECT t2.k, t2.cluster_id, t1.*
  FROM
    mgdemo.mgdata_clean_with_id_tbl t1,
    (SELECT k, cluster_id, bgid FROM mgdemo.mgdata_pgram_array_cluster_id_round03_tbl WHERE k = 8 GROUP BY 1,2,3) t2
  WHERE
    t1.bgid = t2.bgid
DISTRIBUTED BY (k,cluster_id,bgid,rid);
-- Query returned successfully: 521280 rows affected, 34260 ms execution time.
