/*==================================================================================
*         POST-PROCESSING
*
* - Calculate number of periodograms in each cluster.
* - Find cluster assignments for all periodograms.
* - Calculate mean and std deviation of distances in each cluster.
* - Calculate pairwise L2-norm distances between centroids.

*==================================================================================*/

-- TABLE of counts of points in clusters
DROP TABLE IF EXISTS mgdemo.mgdata_cluster_counts_tbl;
CREATE TABLE mgdemo.mgdata_cluster_counts_tbl AS
  SELECT k, cluster_id, count(bgid) as ct_bgid
  FROM mgdemo.mgdata_pgram_array_cluster_id_tbl
  GROUP BY k, cluster_id
DISTRIBUTED RANDOMLY;
--

-- Query returned successfully: 117 rows affected, 259 ms execution time.


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
-- 442;"{3,4,5,6,7,8,9,10,11,12,13,14,15}";13

-- 442;"{3,4,5,6,7,8,9,10,11,12,13,14,15}";13


-- Function to compute silhouette coefficients for each k
CREATE OR REPLACE FUNCTION mgdemo.calc_silhouette_coef_fn(
  input_pgramdata_table_with_schema VARCHAR,
  input_pgramdata_colname VARCHAR,
  input_kmeansout_table_with_schema VARCHAR,
  input_centroids_colname VARCHAR,
  input_fn_dist VARCHAR,
  output_silhcoef_table_with_schema VARCHAR
) RETURNS VOID AS
$$
  DECLARE
      sql TEXT;
      numk INT;
      k_array INT[];
      silh_coef DOUBLE PRECISION;
  BEGIN
      sql := 'DROP TABLE IF EXISTS ' || output_silhcoef_table_with_schema || ';';
      EXECUTE sql;

      sql := 'CREATE TABLE ' || output_silhcoef_table_with_schema
                || ' (k INT, silhouette_coef DOUBLE PRECISION)'
                || ' DISTRIBUTED RANDOMLY;';
      EXECUTE sql;

      -- Assuming each row of the kmeans output table corresponds to a different k value
      sql := 'SELECT COUNT(*) FROM ' || input_kmeansout_table_with_schema || ';';
      EXECUTE sql INTO numk;

      sql := 'SELECT array_agg(k ORDER BY k) FROM ' || input_kmeansout_table_with_schema || ';';
      EXECUTE sql INTO k_array;

      FOR i IN 1..numk LOOP
          RAISE INFO '===== i = % ==== k = % ===== START =====', i, k_array[i];

          sql := 'SELECT madlib.simple_silhouette ('
                  || ' ''' || input_pgramdata_table_with_schema || ''','
                  || ' ''pgram'','
                  || ' (SELECT centroids FROM ' || input_kmeansout_table_with_schema || ' WHERE k = ' || k_array[i] || '),'
                  || ' ''madlib.dist_norm2'')';
          RAISE INFO '===== query =====';
          RAISE INFO '%', sql;
          EXECUTE sql INTO silh_coef;
          RAISE INFO '===== silhouette coefficeint = % =====', silh_coef;

          sql := 'INSERT INTO ' || output_silhcoef_table_with_schema
                    || ' SELECT'
                    || ' ' || k_array[i] || ' AS k,'
                    || ' ' || silh_coef || ' AS silhouette_coef;';
          RAISE INFO '===== query =====';
          RAISE INFO '%', sql;
          EXECUTE sql;
          RAISE INFO '===== i = % ==== k = % ===== END =====', i, k_array[i];
      END LOOP;
  END;
$$
LANGUAGE 'plpgsql';


-- Compute silhouette coefficients for each k
SELECT mgdemo.calc_silhouette_coef_fn(
  'mgdemo.mgdata_pgram_array_tbl',
  'pgram',
  'mgdemo.mgdata_kmeans_output_tbl',
  'centroids',
  'madlib.dist_norm2',
  'mgdemo.mgdata_km_silhcoef_tbl'
);



-- Total query runtime: 3189 ms.
-- 1 row retrieved.

-- Error encountered when function did "insert into <table> select madlib.simple_silhouette "
-- INFO:  ===== i = 1 ==== k = 3 ===== START =====
-- INFO:  ===== query =====
-- INFO:  INSERT INTO mgdemo.mgdata_km_silhcoef_tbl SELECT 3 AS k, madlib.simple_silhouette ( 'mgdemo.mgdata_pgram_array_tbl', 'pgram', (SELECT centroids FROM mgdemo.mgdata_kmeans_output_tbl WHERE k = 3), 'madlib.dist_norm2')
-- ERROR:  function cannot execute on segment because it issues a non-SELECT statement (functions.c:135)  (seg0 slice2 gpdb-sandbox.localdomain:40000 pid=7641) (cdbdisp.c:1320)
-- DETAIL:
-- SQL statement " SHOW optimizer "
-- PL/pgSQL function "simple_silhouette" line 13 at execute statement
-- CONTEXT:  SQL statement "INSERT INTO mgdemo.mgdata_km_silhcoef_tbl SELECT 3 AS k, madlib.simple_silhouette ( 'mgdemo.mgdata_pgram_array_tbl', 'pgram', (SELECT centroids FROM mgdemo.mgdata_kmeans_output_tbl WHERE k = 3), 'madlib.dist_norm2')"
-- PL/pgSQL function "calc_silhouette_coef_fn" line 31 at execute statement
-- ********** Error **********
--
-- ERROR: function cannot execute on segment because it issues a non-SELECT statement (functions.c:135)  (seg0 slice2 gpdb-sandbox.localdomain:40000 pid=7641) (cdbdisp.c:1320)
-- SQL state: XX000
-- Detail:
-- SQL statement " SHOW optimizer "
-- PL/pgSQL function "simple_silhouette" line 13 at execute statement
-- Context: SQL statement "INSERT INTO mgdemo.mgdata_km_silhcoef_tbl SELECT 3 AS k, madlib.simple_silhouette ( 'mgdemo.mgdata_pgram_array_tbl', 'pgram', (SELECT centroids FROM mgdemo.mgdata_kmeans_output_tbl WHERE k = 3), 'madlib.dist_norm2')"
-- PL/pgSQL function "calc_silhouette_coef_fn" line 31 at execute statement


-- Compute SSE (Sum of Squared Errors) for each k
DROP TABLE if EXISTS mgdemo.mgdata_km_sse_tbl;
CREATE TABLE mgdemo.mgdata_km_sse_tbl AS
  SELECT
    k,
    sum(point_to_centroid_error) AS sse
  FROM
  (
    SELECT
      clust_id_tbl.k,
      clust_id_tbl.bgid,
      clust_id_tbl.cluster_id,
      madlib.array_sum(madlib.array_square(madlib.array_sub(pgram,centroid_array))) AS point_to_centroid_error
    FROM
      mgdemo.mgdata_pgram_array_cluster_id_tbl AS clust_id_tbl,
      mgdemo.mgdata_km_centroids_unnest_onelevel_cluster_id_tbl AS kmeans_cent_unnest_one_tbl
    WHERE
      clust_id_tbl.k = kmeans_cent_unnest_one_tbl.k
      AND clust_id_tbl.cluster_id = kmeans_cent_unnest_one_tbl.cluster_id
  ) t1
GROUP BY k
DISTRIBUTED RANDOMLY;
--

-- Query returned successfully: 13 rows affected, 14302 ms execution time.


-- TABLE of distances between pgrams and respective cluster centroids
DROP TABLE IF EXISTS mgdemo.mgdata_pgram_array_centroid_dist_tbl;
CREATE TABLE mgdemo.mgdata_pgram_array_centroid_dist_tbl AS
  SELECT
    t1.k,
    t2.cluster_id,
    bgid,
    sqrt(madlib.array_dot(madlib.array_sub(km_cent,km_pnts),madlib.array_sub(km_cent,km_pnts))) AS l2dist
  FROM (
    SELECT k, cluster_id, centroid_array::float8[] AS km_cent
    FROM mgdemo.mgdata_km_centroids_unnest_onelevel_cluster_id_tbl
  ) t1,
  (
    SELECT k, cluster_id, bgid, pgram::float8[] AS km_pnts
    FROM mgdemo.mgdata_pgram_array_cluster_id_tbl
  ) t2
  WHERE
    t1.k = t2.k
    AND t1.cluster_id = t2.cluster_id
DISTRIBUTED BY (k,cluster_id,bgid);
--

-- Query returned successfully: 26714 rows affected, 4342 ms execution time.


-- TABLE of meter readings AND respective cluster assignments.
DROP TABLE IF EXISTS mgdemo.mgdata_pgram_array_unnest_cluster_id_tbl;
CREATE TABLE mgdemo.mgdata_pgram_array_unnest_cluster_id_tbl AS
  SELECT
    k,
    cluster_id,
    bgid,
    unnest(ridarray) AS point_id,
    unnest(pgram) AS pgram_points
  FROM (
    SELECT
      k, cluster_id, bgid, pgram::float8[], ridarray
    FROM
      mgdemo.mgdata_pgram_array_cluster_id_tbl t1,
      (
        SELECT array_agg(rid ORDER BY rid) AS ridarray
        FROM (
          SELECT generate_series(1,array_upper(pgram,1),1) AS rid
          FROM (
            SELECT pgram FROM mgdemo.mgdata_pgram_array_cluster_id_tbl LIMIT 1
          ) t2
        ) t3
      ) t4
  ) t5
DISTRIBUTED BY (k,cluster_id,bgid,point_id);
-- Query returned successfully: 4137120 rows affected, 319311 ms execution time.

-- Query returned successfully: 4137120 rows affected, 165498 ms execution time.


-- Take k=7 and do further POST-PROCESSING analyses
DROP TABLE IF EXISTS mgdemo.mgdata_pgram_array_unnest_cluster_id_fork7_tbl;
CREATE TABLE mgdemo.mgdata_pgram_array_unnest_cluster_id_fork7_tbl AS
  SELECT *
  FROM mgdemo.mgdata_pgram_array_unnest_cluster_id_tbl
  WHERE k = 7
DISTRIBUTED BY (k,cluster_id,bgid,point_id);
-- NOTICE:  table "mgdata_pgram_array_unnest_cluster_id_fork7_tbl" does not exist, skipping
-- Query returned successfully: 318240 rows affected, 1204 ms execution time.


DROP TABLE IF EXISTS mgdemo.mgdata_clean_with_id_and_cluster_id_fork7_tbl;
CREATE TABLE mgdemo.mgdata_clean_with_id_and_cluster_id_fork7_tbl AS
  SELECT t2.k, t2.cluster_id, t1.*
  FROM
    mgdemo.mgdata_clean_with_id_tbl t1,
    (SELECT k, cluster_id, bgid FROM mgdemo.mgdata_pgram_array_cluster_id_tbl WHERE k = 7 GROUP BY 1,2,3) t2
  WHERE
    t1.bgid = t2.bgid
DISTRIBUTED BY (k,cluster_id,bgid,rid);
-- NOTICE:  table "mgdata_clean_with_id_and_cluster_id_fork7_tbl" does not exist, skipping
-- Query returned successfully: 636480 rows affected, 18389 ms execution time.
