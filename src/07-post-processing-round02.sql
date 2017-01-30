/*==================================================================================
*         POST-PROCESSING
*
* - Calculate number of periodograms in each cluster.
* - Find cluster assignments for all periodograms.
* - Calculate mean and std deviation of distances in each cluster.
* - Calculate pairwise L2-norm distances between centroids.

*==================================================================================*/

-- TABLE of counts of points in clusters
DROP TABLE IF EXISTS mgdemo.mgdata_cluster_counts_round02_tbl;
CREATE TABLE mgdemo.mgdata_cluster_counts_round02_tbl AS
  SELECT k, cluster_id, count(bgid) as ct_bgid
  FROM mgdemo.mgdata_pgram_array_cluster_id_round02_tbl
  GROUP BY k, cluster_id
DISTRIBUTED RANDOMLY;
-- Query returned successfully: 117 rows affected, 273 ms execution time.


-- Check to make sure that the total number of bgid(s) is 442
-- (the number of data points that went in to clustering)
SELECT sum_ct_bgid, array_agg(k ORDER BY k) as array_k, COUNT(k) as ct_k
FROM (
  SELECT k, sum(ct_bgid) as sum_ct_bgid
  FROM mgdemo.mgdata_cluster_counts_round02_tbl
  GROUP BY k
) t1
GROUP BY sum_ct_bgid
ORDER BY sum_ct_bgid;
-- 429;"{3,4,5,6,7,8,9,10,11,12,13,14,15}";13


-- Compute silhouette coefficients for each k
SELECT mgdemo.calc_silhouette_coef_fn(
  'mgdemo.mgdata_pgram_array_round02_tbl',
  'pgram',
  'mgdemo.mgdata_kmeans_output_round02_tbl',
  'centroids',
  'madlib.dist_norm2',
  'mgdemo.mgdata_km_silhcoef_round02_tbl'
);
-- INFO:  ===== i = 13 ==== k = 15 ===== START =====
-- INFO:  ===== query =====
-- INFO:  SELECT madlib.simple_silhouette ( 'mgdemo.mgdata_pgram_array_round02_tbl', 'pgram', (SELECT centroids FROM mgdemo.mgdata_kmeans_output_round02_tbl WHERE k = 15), 'madlib.dist_norm2')
-- INFO:  ===== silhouette coefficeint = 0.62275640624467 =====
-- INFO:  ===== query =====
-- INFO:  INSERT INTO mgdemo.mgdata_km_silhcoef_round02_tbl SELECT 15 AS k, 0.62275640624467 AS silhouette_coef;
-- INFO:  ===== i = 13 ==== k = 15 ===== END =====
-- Total query runtime: 1341 ms.
-- 1 row retrieved.



-- Compute SSE (Sum of Squared Errors) for each k
DROP TABLE if EXISTS mgdemo.mgdata_km_sse_round02_tbl;
CREATE TABLE mgdemo.mgdata_km_sse_round02_tbl AS
  SELECT
    k,
    sum(point_to_centroid_error) AS sse
  FROM
  (
    SELECT
      clust_id_tbl.k,
      clust_id_tbl.bgid,
      clust_id_tbl.bgid_r2,
      clust_id_tbl.cluster_id,
      madlib.array_sum(madlib.array_square(madlib.array_sub(pgram,centroid_array))) AS point_to_centroid_error
    FROM
      mgdemo.mgdata_pgram_array_cluster_id_round02_tbl AS clust_id_tbl,
      mgdemo.mgdata_km_centroids_unnest_onelevel_cluster_id_round02_tbl AS kmeans_cent_unnest_one_tbl
    WHERE
      clust_id_tbl.k = kmeans_cent_unnest_one_tbl.k
      AND clust_id_tbl.cluster_id = kmeans_cent_unnest_one_tbl.cluster_id
  ) t1
GROUP BY k
DISTRIBUTED RANDOMLY;
-- Query returned successfully: 13 rows affected, 488 ms execution time.


select *, sse/1e9 from mgdemo.mgdata_km_sse_round02_tbl
order by k;
/*
3;733819957.422743;0.733819957422743
4;170783267.727694;0.170783267727694
5;131882649.546807;0.131882649546807
6;114812572.163075;0.114812572163075
7;111724284.552702;0.111724284552702
8;95856063.115184;0.095856063115184
9;84744482.0804373;0.0847444820804373
10;78951188.4233557;0.0789511884233557
11;82548100.9301161;0.0825481009301161
12;71468965.4569058;0.0714689654569058
13;60388926.6566029;0.0603889266566029
14;55954429.020416;0.055954429020416
15;48299461.1347456;0.0482994611347456
*/

-- After looking at the above SSE values (the elbow plot) and the cluster count distributions,
-- heuristically we choose k=5
SELECT * FROM mgdemo.mgdata_cluster_counts_round02_tbl
WHERE k=5 ORDER BY cluster_id;
-- k;cluster_id;ct_bgid
-- 5;0;10
-- 5;1;48
-- 5;2;8
-- 5;3;362
-- 5;4;1

-- Take k=5 here itself
-- TABLE of distances between pgrams and respective cluster centroids
DROP TABLE IF EXISTS mgdemo.mgdata_pgram_array_centroid_dist_round02_fork5_tbl;
CREATE TABLE mgdemo.mgdata_pgram_array_centroid_dist_round02_fork5_tbl AS
  SELECT
    t1.k,
    t2.cluster_id,
    bgid,
    bgid_r2,
    sqrt(madlib.array_dot(madlib.array_sub(km_cent,km_pnts),madlib.array_sub(km_cent,km_pnts))) AS l2dist
  FROM (
    SELECT k, cluster_id, centroid_array::float8[] AS km_cent
    FROM mgdemo.mgdata_km_centroids_unnest_onelevel_cluster_id_round02_tbl
    WHERE k=5
  ) t1,
  (
    SELECT k, cluster_id, bgid, bgid_r2, pgram::float8[] AS km_pnts
    FROM mgdemo.mgdata_pgram_array_cluster_id_round02_tbl
    WHERE k=5
  ) t2
  WHERE
    t1.k = t2.k
    AND t1.cluster_id = t2.cluster_id
DISTRIBUTED BY (k,cluster_id,bgid_r2);
-- Query returned successfully: 429 rows affected, 997 ms execution time.


-- TABLE of meter readings AND respective cluster assignments.
DROP TABLE IF EXISTS mgdemo.mgdata_pgram_array_unnest_cluster_id_round02_fork5_tbl;
CREATE TABLE mgdemo.mgdata_pgram_array_unnest_cluster_id_round02_fork5_tbl AS
  SELECT
    k,
    cluster_id,
    bgid,
    bgid_r2,
    unnest(ridarray) AS point_id,
    unnest(pgram) AS pgram_points
  FROM (
    SELECT
      k, cluster_id, bgid, bgid_r2, pgram::float8[], ridarray
    FROM
      (
        SELECT * FROM mgdemo.mgdata_pgram_array_cluster_id_round02_tbl
        WHERE k=5
      ) t1,
      (
        SELECT array_agg(rid ORDER BY rid) AS ridarray
        FROM (
          SELECT generate_series(1,array_upper(pgram,1),1) AS rid
          FROM (
            SELECT pgram FROM mgdemo.mgdata_pgram_array_cluster_id_round02_tbl
            WHERE k=5 LIMIT 1
          ) t2
        ) t3
      ) t4
  ) t5
DISTRIBUTED BY (k,cluster_id,bgid,point_id);
-- Query returned successfully: 308880 rows affected, 1619 ms execution time.


DROP TABLE IF EXISTS mgdemo.mgdata_clean_with_id_and_cluster_id_round02_fork5_tbl;
CREATE TABLE mgdemo.mgdata_clean_with_id_and_cluster_id_round02_fork5_tbl AS
  SELECT t2.k, t2.cluster_id, t1.*
  FROM
    mgdemo.mgdata_clean_with_id_tbl t1,
    (SELECT k, cluster_id, bgid FROM mgdemo.mgdata_pgram_array_cluster_id_round02_tbl WHERE k = 5 GROUP BY 1,2,3) t2
  WHERE
    t1.bgid = t2.bgid
DISTRIBUTED BY (k,cluster_id,bgid,rid);
-- Query returned successfully: 617760 rows affected, 122446 ms execution time.
