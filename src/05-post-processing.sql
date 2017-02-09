/*==================================================================================
*         POST-PROCESSING
*
* - Calculate number of periodograms in each cluster.
* - Find cluster assignments for all periodograms.
* - Calculate mean and std deviation of distances in each cluster.
* - Calculate pairwise L2-norm distances between centroids.

*==================================================================================*/

-- Unnesting 3 rounds of centroids by one level (2 steps per round)
-- Round 1 --
drop table if exists pt.mgdata_km_centroids_unnest_full_tbl;
CREATE TABLE pt.mgdata_km_centroids_unnest_full_tbl AS
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
      FROM pt.kmeans_output_tbl
    ) t1
  ) t2
DISTRIBUTED BY (k,array_id,index_id);

drop table if exists pt.mgdata_km_centroids_unnest_onelevel_cluster_id_tbl;
CREATE TABLE pt.mgdata_km_centroids_unnest_onelevel_cluster_id_tbl AS
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
        pt.mgdata_km_centroids_unnest_full_tbl
      GROUP BY
        k, array_id
    ) t1,
    pt.kmeans_output_tbl t2
    WHERE t1.k = t2.k
  ) t2
DISTRIBUTED by (cluster_id);


-- Round 2 --
drop table if exists pt.mgdata_km_centroids_unnest_full_r2_tbl;
CREATE TABLE pt.mgdata_km_centroids_unnest_full_r2_tbl AS
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
      FROM pt.kmeans_output_r2_tbl
    ) t1
  ) t2
DISTRIBUTED BY (k,array_id,index_id);

drop table if exists pt.mgdata_km_centroids_unnest_onelevel_cluster_id_r2_tbl;
CREATE TABLE pt.mgdata_km_centroids_unnest_onelevel_cluster_id_r2_tbl AS
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
        pt.mgdata_km_centroids_unnest_full_r2_tbl
      GROUP BY
        k, array_id
    ) t1,
    pt.kmeans_output_r2_tbl t2
    WHERE t1.k = t2.k
  ) t2
DISTRIBUTED by (cluster_id);


-- Round 3 --
drop table if exists pt.mgdata_km_centroids_unnest_full_r3_tbl;
CREATE TABLE pt.mgdata_km_centroids_unnest_full_r3_tbl AS
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
      FROM pt.kmeans_output_r3_tbl
    ) t1
  ) t2
DISTRIBUTED BY (k,array_id,index_id);

drop table if exists pt.mgdata_km_centroids_unnest_onelevel_cluster_id_r3_tbl;
CREATE TABLE pt.mgdata_km_centroids_unnest_onelevel_cluster_id_r3_tbl AS
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
        pt.mgdata_km_centroids_unnest_full_r3_tbl
      GROUP BY
        k, array_id
    ) t1,
    pt.kmeans_output_r3_tbl t2
    WHERE t1.k = t2.k
  ) t2
DISTRIBUTED by (cluster_id);


-- Computing l2dist between centroids and all data points
-- Round 1 --
DROP TABLE IF EXISTS pt.mgdata_pgram_norm_array_cluster_id_l2dist_tbl;
CREATE TABLE pt.mgdata_pgram_norm_array_cluster_id_l2dist_tbl AS
  SELECT
    t2.*,
    km_centroid || madlib.array_of_float(array_upper(km_centroid, 1)) as km_centroid_padded,
    sqrt(madlib.array_dot(madlib.array_sub(km_centroid, pgram_norm_arr),madlib.array_sub(km_centroid, pgram_norm_arr))) AS l2dist
  FROM (
    SELECT k, cluster_id, centroid_array::float8[] AS km_centroid
    FROM pt.mgdata_km_centroids_unnest_onelevel_cluster_id_tbl
  ) t1,
  (
    SELECT
      k,
      cluster_id,
      bgid,
      building_num,
      usagekw_sum_5min_mean,
      usagekw_sum_5min_norm_denom,
      win_id_arr,
      usagekw_sum_5min_arr,
      usagekw_sum_5min_norm_arr,
      pgram_pt_id_arr,
      pgram_norm_arr,
      pgram_pt_id_arr_padded,
      pgram_norm_arr_padded
    FROM pt.mgdata_pgram_norm_array_cluster_id_tbl
  ) t2
  WHERE
    t1.k = t2.k
    AND t1.cluster_id = t2.cluster_id
DISTRIBUTED BY (k,cluster_id,bgid);

-- Round 2 --
DROP TABLE IF EXISTS pt.mgdata_pgram_norm_array_cluster_id_l2dist_r2_tbl;
CREATE TABLE pt.mgdata_pgram_norm_array_cluster_id_l2dist_r2_tbl AS
  SELECT
    t2.*,
    km_centroid || madlib.array_of_float(array_upper(km_centroid, 1)) as km_centroid_padded,
    sqrt(madlib.array_dot(madlib.array_sub(km_centroid, pgram_norm_arr),madlib.array_sub(km_centroid, pgram_norm_arr))) AS l2dist
  FROM (
    SELECT k, cluster_id, centroid_array::float8[] AS km_centroid
    FROM pt.mgdata_km_centroids_unnest_onelevel_cluster_id_r2_tbl
  ) t1,
  (
    SELECT
      k,
      cluster_id,
      bgid,
      building_num,
      usagekw_sum_5min_mean,
      usagekw_sum_5min_norm_denom,
      win_id_arr,
      usagekw_sum_5min_arr,
      usagekw_sum_5min_norm_arr,
      pgram_pt_id_arr,
      pgram_norm_arr,
      pgram_pt_id_arr_padded,
      pgram_norm_arr_padded
    FROM pt.mgdata_pgram_norm_array_cluster_id_r2_tbl
  ) t2
  WHERE
    t1.k = t2.k
    AND t1.cluster_id = t2.cluster_id
DISTRIBUTED BY (k,cluster_id,bgid);

-- Round 3 --
DROP TABLE IF EXISTS pt.mgdata_pgram_norm_array_cluster_id_l2dist_r3_tbl;
CREATE TABLE pt.mgdata_pgram_norm_array_cluster_id_l2dist_r3_tbl AS
  SELECT
    t2.*,
    km_centroid || madlib.array_of_float(array_upper(km_centroid, 1)) as km_centroid_padded,
    sqrt(madlib.array_dot(madlib.array_sub(km_centroid, pgram_norm_arr),madlib.array_sub(km_centroid, pgram_norm_arr))) AS l2dist
  FROM (
    SELECT k, cluster_id, centroid_array::float8[] AS km_centroid
    FROM pt.mgdata_km_centroids_unnest_onelevel_cluster_id_r3_tbl
  ) t1,
  (
    SELECT
      k,
      cluster_id,
      bgid,
      building_num,
      usagekw_sum_5min_mean,
      usagekw_sum_5min_norm_denom,
      win_id_arr,
      usagekw_sum_5min_arr,
      usagekw_sum_5min_norm_arr,
      pgram_pt_id_arr,
      pgram_norm_arr,
      pgram_pt_id_arr_padded,
      pgram_norm_arr_padded
    FROM pt.mgdata_pgram_norm_array_cluster_id_r3_tbl
  ) t2
  WHERE
    t1.k = t2.k
    AND t1.cluster_id = t2.cluster_id
DISTRIBUTED BY (k,cluster_id,bgid);
-- Query returned successfully: 198 rows affected, 12971 ms execution time.


-- Join all cluster levels in one table
drop table if exists pt.mgdata_pgram_norm_array_cluster_id_l2dist_allrounds_tbl;
create table pt.mgdata_pgram_norm_array_cluster_id_l2dist_allrounds_tbl as
  select
    t1.bgid,
    t1.building_num,
    t1.usagekw_sum_5min_mean,
    t1.usagekw_sum_5min_norm_denom,
    t1.win_id_arr,
    t1.usagekw_sum_5min_arr,
    t1.usagekw_sum_5min_norm_arr,
    t1.pgram_pt_id_arr,
    t1.pgram_norm_arr,
    t1.pgram_pt_id_arr_padded,
    t1.pgram_norm_arr_padded,
    t1.k,
    t1.cluster_id,
    t1.l2dist,
    t1.km_centroid_padded,
    u2.k_r2,
    u2.cluster_id_r2,
    u2.l2dist_r2,
    u2.km_centroid_padded_r2,
    v3.k_r3,
    v3.cluster_id_r3,
    v3.l2dist_r3,
    v3.km_centroid_padded_r3
  from
  (
    select
      k,
      cluster_id,
      bgid,
      building_num,
      usagekw_sum_5min_mean,
      usagekw_sum_5min_norm_denom,
      win_id_arr,
      usagekw_sum_5min_arr,
      usagekw_sum_5min_norm_arr,
      pgram_pt_id_arr,
      pgram_norm_arr,
      pgram_pt_id_arr_padded,
      pgram_norm_arr_padded,
      km_centroid_padded,
      l2dist
    from
      pt.mgdata_pgram_norm_array_cluster_id_l2dist_tbl
  ) t1
  full outer Join
  (
    select
      k as k_r2,
      cluster_id as cluster_id_r2,
      bgid,
      building_num,
      usagekw_sum_5min_mean,
      usagekw_sum_5min_norm_denom,
      win_id_arr,
      usagekw_sum_5min_arr,
      usagekw_sum_5min_norm_arr,
      pgram_pt_id_arr,
      pgram_norm_arr,
      pgram_pt_id_arr_padded,
      pgram_norm_arr_padded,
      km_centroid_padded as km_centroid_padded_r2,
      l2dist as l2dist_r2
    from
      pt.mgdata_pgram_norm_array_cluster_id_l2dist_r2_tbl
  ) u2
  using (bgid)
  full outer Join
  (
    select
      k as k_r3,
      cluster_id as cluster_id_r3,
      bgid,
      building_num,
      usagekw_sum_5min_mean,
      usagekw_sum_5min_norm_denom,
      win_id_arr,
      usagekw_sum_5min_arr,
      usagekw_sum_5min_norm_arr,
      pgram_pt_id_arr,
      pgram_norm_arr,
      pgram_pt_id_arr_padded,
      pgram_norm_arr_padded,
      km_centroid_padded as km_centroid_padded_r3,
      l2dist as l2dist_r3
    from
      pt.mgdata_pgram_norm_array_cluster_id_l2dist_r3_tbl
  ) v3
  using (bgid)
DISTRIBUTED by (bgid);
-- Query returned successfully: 388 rows affected, 7196 ms execution time.


-- Put in overall cluster IDs
drop table if exists pt.mgdata_pgram_norm_array_cluster_id_l2dist_allrounds_wid_tbl;
create table pt.mgdata_pgram_norm_array_cluster_id_l2dist_allrounds_wid_tbl as
  select
    bgid,
    cluster_id_all,
    building_num,
    usagekw_sum_5min_mean,
    usagekw_sum_5min_norm_denom,
    win_id_arr,
    usagekw_sum_5min_arr,
    usagekw_sum_5min_norm_arr,
    pgram_pt_id_arr_padded,
    pgram_norm_arr_padded,
    case
      when l2dist_r3 is NULL and l2dist_r2 is NULL then l2dist
      when l2dist_r3 is NULL and l2dist_r2 is NOT NULL then l2dist_r2
      when l2dist_r3 is NOT NULL then l2dist_r3
    end as l2dist_all,
    k,
    t1.cluster_id,
    l2dist,
    km_centroid_padded,
    k_r2,
    t1.cluster_id_r2_nonull,
    l2dist_r2,
    coalesce(km_centroid_padded_r2,
      madlib.array_fill(madlib.array_of_float(array_upper(pgram_norm_arr_padded,1)),-9999::float8)) as km_centroid_padded_r2_nonull,
    k_r3,
    t1.cluster_id_r3_nonull,
    l2dist_r3,
    coalesce(km_centroid_padded_r3,
      madlib.array_fill(madlib.array_of_float(array_upper(pgram_norm_arr_padded,1)),-9999::float8)) as km_centroid_padded_r3_nonull
  from
  (
    select
      *,
      coalesce(cluster_id_r2,-9999) as cluster_id_r2_nonull,
      coalesce(cluster_id_r3,-9999) as cluster_id_r3_nonull
    from pt.mgdata_pgram_norm_array_cluster_id_l2dist_allrounds_tbl
  ) t1,
  (
    select
      row_number() over (order by cluster_id, cluster_id_r2, cluster_id_r3) - 1 as cluster_id_all,
      cluster_id,
      cluster_id_r2,
      cluster_id_r3,
      coalesce(cluster_id_r2,-9999) as cluster_id_r2_nonull,
      coalesce(cluster_id_r3,-9999) as cluster_id_r3_nonull
    from
    (
      select cluster_id, cluster_id_r2, cluster_id_r3
      from pt.mgdata_pgram_norm_array_cluster_id_l2dist_allrounds_tbl
      group by 1,2,3
    ) t3
  ) t2
  where t1.cluster_id = t2.cluster_id
    and t1.cluster_id_r2_nonull = t2.cluster_id_r2_nonull
    and t1.cluster_id_r3_nonull = t2.cluster_id_r3_nonull
DISTRIBUTED by (bgid);
-- Query returned successfully: 388 rows affected, 8510 ms execution time.


-- Unnest time series signal and periodogram from combined table above
drop table if exists pt.mgdata_pgram_norm_unnest_cluster_id_l2dist_allrounds_wid_tbl;
create table pt.mgdata_pgram_norm_unnest_cluster_id_l2dist_allrounds_wid_tbl as
  select
    bgid,
    cluster_id_all,
    building_num,
    usagekw_sum_5min_mean,
    usagekw_sum_5min_norm_denom,
    unnest(win_id_arr) as win_id,
    unnest(usagekw_sum_5min_arr) as usagekw_sum_5min,
    unnest(usagekw_sum_5min_norm_arr) as usagekw_sum_5min_norm,
    unnest(pgram_pt_id_arr_padded) as pgram_pt_id_padded,
    unnest(pgram_norm_arr_padded) as pgram_norm_padded,
    l2dist_all,
    k,
    cluster_id,
    l2dist,
    unnest(km_centroid_padded) as km_centroid_padded_unnest,
    k_r2,
    cluster_id_r2_nonull,
    l2dist_r2,
    unnest(km_centroid_padded_r2_nonull) as km_centroid_padded_unnest_r2_nonull,
    k_r3,
    cluster_id_r3_nonull,
    l2dist_r3,
    unnest(km_centroid_padded_r3_nonull) as km_centroid_padded_unnest_r3_nonull
  from
    pt.mgdata_pgram_norm_array_cluster_id_l2dist_allrounds_wid_tbl
distributed by (bgid, win_id);
-- Query returned successfully: 111744 rows affected, 15039 ms execution time.


-- For visualization only
drop table if exists pt.mgdata_cluster_id_l2dist_allrounds_wid_viz_tbl;
create table pt.mgdata_cluster_id_l2dist_allrounds_wid_viz_tbl as
  select
    t1.bgid,
    t1.cluster_id_all,
    cluster_id,
    cluster_id_r2_nonull,
    cluster_id_r3_nonull,
    l2dist_all,
    l2dist_max,
    theta,
    l2dist_all*cos(theta) as l2dist_xcoord,
    l2dist_all*sin(theta) as l2dist_ycoord,
    case  when l2dist_max <> 0 then (l2dist_all/l2dist_max)*cos(theta)
          else 0
          end as l2dist_rel_xcoord,
    case  when l2dist_max <> 0 then (l2dist_all/l2dist_max)*sin(theta)
          else 0
          end as l2dist_rel_ycoord
  from
    pt.mgdata_pgram_norm_array_cluster_id_l2dist_allrounds_wid_tbl t1,
    (
      select bgid, random()*2*pi() as theta
      from
      (
        select bgid from pt.mgdata_pgram_norm_array_cluster_id_l2dist_allrounds_wid_tbl group by 1
      ) t2
    ) t3,
    (
      -- max l2dist per cluster
      select cluster_id_all, max(l2dist) as l2dist_max
      from pt.mgdata_pgram_norm_array_cluster_id_l2dist_allrounds_wid_tbl
      group by 1
    ) t4
  where t1.bgid = t3.bgid
    and t1.cluster_id_all = t4.cluster_id_all
distributed by (bgid);
-- Query returned successfully: 388 rows affected, 1327 ms execution time.

-- Get separate centroids unnested table for viz only
drop table if exists pt.mgdata_cluster_id_l2dist_allrounds_wid_centarray_viz_tbl;
create table pt.mgdata_cluster_id_l2dist_allrounds_wid_centarray_viz_tbl as
  select
    cluster_id_all,
    cluster_id,
    cluster_id_r2_nonull,
    cluster_id_r3_nonull,
    pgram_pt_id_arr_padded,
    km_centroid_padded,
    km_centroid_padded_r2_nonull,
    km_centroid_padded_r3_nonull,
    case
      when cluster_id_r3_nonull = -9999 and cluster_id_r2_nonull = -9999 then km_centroid_padded
      when cluster_id_r3_nonull = -9999 and cluster_id_r2_nonull <> -9999 then km_centroid_padded_r2_nonull
      when cluster_id_r3_nonull <> -9999 then km_centroid_padded_r3_nonull
    end as km_centroid_padded_allrounds
  from
  (
    select
      cluster_id_all,
      cluster_id,
      cluster_id_r2_nonull,
      cluster_id_r3_nonull,
      pgram_pt_id_arr_padded,
      km_centroid_padded,
      km_centroid_padded_r2_nonull,
      km_centroid_padded_r3_nonull
    from
      pt.mgdata_pgram_norm_array_cluster_id_l2dist_allrounds_wid_tbl
    group by
      1,2,3,4,5,6,7,8
  ) t1
distributed by (cluster_id_all);

drop table if exists pt.mgdata_cluster_id_l2dist_allrounds_wid_centunnest_viz_tbl;
create table pt.mgdata_cluster_id_l2dist_allrounds_wid_centunnest_viz_tbl as
  select
    cluster_id_all,
    cluster_id,
    cluster_id_r2_nonull,
    cluster_id_r3_nonull,
    unnest(pgram_pt_id_arr_padded) as pgram_pt_id_padded,
    unnest(km_centroid_padded) as km_centroid_padded_unnest,
    unnest(km_centroid_padded_r2_nonull) as km_centroid_padded_unnest_r2_nonull,
    unnest(km_centroid_padded_r3_nonull) as km_centroid_padded_unnest_r3_nonull,
    unnest(km_centroid_padded_allrounds) as km_centroid_padded_unnest_allrounds
  from
    pt.mgdata_cluster_id_l2dist_allrounds_wid_centarray_viz_tbl
distributed by (cluster_id_all, pgram_pt_id_padded);
-- Query returned successfully: 8064 rows affected, 947 ms execution time.




-- -- TABLE of counts of points in clusters
-- DROP TABLE IF EXISTS mgdemo.mgdata_cluster_counts_tbl;
-- CREATE TABLE mgdemo.mgdata_cluster_counts_tbl AS
--   SELECT k, cluster_id, count(bgid) as ct_bgid
--   FROM mgdemo.mgdata_pgram_array_cluster_id_tbl
--   GROUP BY k, cluster_id
-- DISTRIBUTED RANDOMLY;
-- --
--
-- -- Query returned successfully: 117 rows affected, 259 ms execution time.
--
--
-- -- Check to make sure that the total number of bgid(s) is 442
-- -- (the number of data points that went in to clustering)
-- SELECT sum_ct_bgid, array_agg(k ORDER BY k) as array_k, COUNT(k) as ct_k
-- FROM (
--   SELECT k, sum(ct_bgid) as sum_ct_bgid
--   FROM mgdemo.mgdata_cluster_counts_tbl
--   GROUP BY k
-- ) t1
-- GROUP BY sum_ct_bgid
-- ORDER BY sum_ct_bgid;
-- -- 442;"{3,4,5,6,7,8,9,10,11,12,13,14,15}";13
--
-- -- 442;"{3,4,5,6,7,8,9,10,11,12,13,14,15}";13
--
--
-- -- Function to compute silhouette coefficients for each k
-- CREATE OR REPLACE FUNCTION mgdemo.calc_silhouette_coef_fn(
--   input_pgramdata_table_with_schema VARCHAR,
--   input_pgramdata_colname VARCHAR,
--   input_kmeansout_table_with_schema VARCHAR,
--   input_centroids_colname VARCHAR,
--   input_fn_dist VARCHAR,
--   output_silhcoef_table_with_schema VARCHAR
-- ) RETURNS VOID AS
-- $$
--   DECLARE
--       sql TEXT;
--       numk INT;
--       k_array INT[];
--       silh_coef DOUBLE PRECISION;
--   BEGIN
--       sql := 'DROP TABLE IF EXISTS ' || output_silhcoef_table_with_schema || ';';
--       EXECUTE sql;
--
--       sql := 'CREATE TABLE ' || output_silhcoef_table_with_schema
--                 || ' (k INT, silhouette_coef DOUBLE PRECISION)'
--                 || ' DISTRIBUTED RANDOMLY;';
--       EXECUTE sql;
--
--       -- Assuming each row of the kmeans output table corresponds to a different k value
--       sql := 'SELECT COUNT(*) FROM ' || input_kmeansout_table_with_schema || ';';
--       EXECUTE sql INTO numk;
--
--       sql := 'SELECT array_agg(k ORDER BY k) FROM ' || input_kmeansout_table_with_schema || ';';
--       EXECUTE sql INTO k_array;
--
--       FOR i IN 1..numk LOOP
--           RAISE INFO '===== i = % ==== k = % ===== START =====', i, k_array[i];
--
--           sql := 'SELECT madlib.simple_silhouette ('
--                   || ' ''' || input_pgramdata_table_with_schema || ''','
--                   || ' ''pgram'','
--                   || ' (SELECT centroids FROM ' || input_kmeansout_table_with_schema || ' WHERE k = ' || k_array[i] || '),'
--                   || ' ''madlib.dist_norm2'')';
--           RAISE INFO '===== query =====';
--           RAISE INFO '%', sql;
--           EXECUTE sql INTO silh_coef;
--           RAISE INFO '===== silhouette coefficeint = % =====', silh_coef;
--
--           sql := 'INSERT INTO ' || output_silhcoef_table_with_schema
--                     || ' SELECT'
--                     || ' ' || k_array[i] || ' AS k,'
--                     || ' ' || silh_coef || ' AS silhouette_coef;';
--           RAISE INFO '===== query =====';
--           RAISE INFO '%', sql;
--           EXECUTE sql;
--           RAISE INFO '===== i = % ==== k = % ===== END =====', i, k_array[i];
--       END LOOP;
--   END;
-- $$
-- LANGUAGE 'plpgsql';
--
--
-- -- Compute silhouette coefficients for each k
-- SELECT mgdemo.calc_silhouette_coef_fn(
--   'mgdemo.mgdata_pgram_array_tbl',
--   'pgram',
--   'mgdemo.mgdata_kmeans_output_tbl',
--   'centroids',
--   'madlib.dist_norm2',
--   'mgdemo.mgdata_km_silhcoef_tbl'
-- );
--
--
--
-- -- Total query runtime: 3189 ms.
-- -- 1 row retrieved.
--
-- -- Error encountered when function did "insert into <table> select madlib.simple_silhouette "
-- -- INFO:  ===== i = 1 ==== k = 3 ===== START =====
-- -- INFO:  ===== query =====
-- -- INFO:  INSERT INTO mgdemo.mgdata_km_silhcoef_tbl SELECT 3 AS k, madlib.simple_silhouette ( 'mgdemo.mgdata_pgram_array_tbl', 'pgram', (SELECT centroids FROM mgdemo.mgdata_kmeans_output_tbl WHERE k = 3), 'madlib.dist_norm2')
-- -- ERROR:  function cannot execute on segment because it issues a non-SELECT statement (functions.c:135)  (seg0 slice2 gpdb-sandbox.localdomain:40000 pid=7641) (cdbdisp.c:1320)
-- -- DETAIL:
-- -- SQL statement " SHOW optimizer "
-- -- PL/pgSQL function "simple_silhouette" line 13 at execute statement
-- -- CONTEXT:  SQL statement "INSERT INTO mgdemo.mgdata_km_silhcoef_tbl SELECT 3 AS k, madlib.simple_silhouette ( 'mgdemo.mgdata_pgram_array_tbl', 'pgram', (SELECT centroids FROM mgdemo.mgdata_kmeans_output_tbl WHERE k = 3), 'madlib.dist_norm2')"
-- -- PL/pgSQL function "calc_silhouette_coef_fn" line 31 at execute statement
-- -- ********** Error **********
-- --
-- -- ERROR: function cannot execute on segment because it issues a non-SELECT statement (functions.c:135)  (seg0 slice2 gpdb-sandbox.localdomain:40000 pid=7641) (cdbdisp.c:1320)
-- -- SQL state: XX000
-- -- Detail:
-- -- SQL statement " SHOW optimizer "
-- -- PL/pgSQL function "simple_silhouette" line 13 at execute statement
-- -- Context: SQL statement "INSERT INTO mgdemo.mgdata_km_silhcoef_tbl SELECT 3 AS k, madlib.simple_silhouette ( 'mgdemo.mgdata_pgram_array_tbl', 'pgram', (SELECT centroids FROM mgdemo.mgdata_kmeans_output_tbl WHERE k = 3), 'madlib.dist_norm2')"
-- -- PL/pgSQL function "calc_silhouette_coef_fn" line 31 at execute statement
--
--
-- -- Compute SSE (Sum of Squared Errors) for each k
-- DROP TABLE if EXISTS mgdemo.mgdata_km_sse_tbl;
-- CREATE TABLE mgdemo.mgdata_km_sse_tbl AS
--   SELECT
--     k,
--     sum(point_to_centroid_error) AS sse
--   FROM
--   (
--     SELECT
--       clust_id_tbl.k,
--       clust_id_tbl.bgid,
--       clust_id_tbl.cluster_id,
--       madlib.array_sum(madlib.array_square(madlib.array_sub(pgram,centroid_array))) AS point_to_centroid_error
--     FROM
--       mgdemo.mgdata_pgram_array_cluster_id_tbl AS clust_id_tbl,
--       mgdemo.mgdata_km_centroids_unnest_onelevel_cluster_id_tbl AS kmeans_cent_unnest_one_tbl
--     WHERE
--       clust_id_tbl.k = kmeans_cent_unnest_one_tbl.k
--       AND clust_id_tbl.cluster_id = kmeans_cent_unnest_one_tbl.cluster_id
--   ) t1
-- GROUP BY k
-- DISTRIBUTED RANDOMLY;
-- --
--
-- -- Query returned successfully: 13 rows affected, 14302 ms execution time.
--
--
-- -- TABLE of distances between pgrams and respective cluster centroids
-- DROP TABLE IF EXISTS mgdemo.mgdata_pgram_array_centroid_dist_tbl;
-- CREATE TABLE mgdemo.mgdata_pgram_array_centroid_dist_tbl AS
--   SELECT
--     t1.k,
--     t2.cluster_id,
--     bgid,
--     sqrt(madlib.array_dot(madlib.array_sub(km_cent,km_pnts),madlib.array_sub(km_cent,km_pnts))) AS l2dist
--   FROM (
--     SELECT k, cluster_id, centroid_array::float8[] AS km_cent
--     FROM mgdemo.mgdata_km_centroids_unnest_onelevel_cluster_id_tbl
--   ) t1,
--   (
--     SELECT k, cluster_id, bgid, pgram::float8[] AS km_pnts
--     FROM mgdemo.mgdata_pgram_array_cluster_id_tbl
--   ) t2
--   WHERE
--     t1.k = t2.k
--     AND t1.cluster_id = t2.cluster_id
-- DISTRIBUTED BY (k,cluster_id,bgid);
-- --
--
-- -- Query returned successfully: 26714 rows affected, 4342 ms execution time.
--
--
-- -- TABLE of meter readings AND respective cluster assignments.
-- DROP TABLE IF EXISTS mgdemo.mgdata_pgram_array_unnest_cluster_id_tbl;
-- CREATE TABLE mgdemo.mgdata_pgram_array_unnest_cluster_id_tbl AS
--   SELECT
--     k,
--     cluster_id,
--     bgid,
--     unnest(ridarray) AS point_id,
--     unnest(pgram) AS pgram_points
--   FROM (
--     SELECT
--       k, cluster_id, bgid, pgram::float8[], ridarray
--     FROM
--       mgdemo.mgdata_pgram_array_cluster_id_tbl t1,
--       (
--         SELECT array_agg(rid ORDER BY rid) AS ridarray
--         FROM (
--           SELECT generate_series(1,array_upper(pgram,1),1) AS rid
--           FROM (
--             SELECT pgram FROM mgdemo.mgdata_pgram_array_cluster_id_tbl LIMIT 1
--           ) t2
--         ) t3
--       ) t4
--   ) t5
-- DISTRIBUTED BY (k,cluster_id,bgid,point_id);
-- -- Query returned successfully: 4137120 rows affected, 319311 ms execution time.
--
-- -- Query returned successfully: 4137120 rows affected, 165498 ms execution time.
--
--
-- -- Take k=7 and do further POST-PROCESSING analyses
-- DROP TABLE IF EXISTS mgdemo.mgdata_pgram_array_unnest_cluster_id_fork7_tbl;
-- CREATE TABLE mgdemo.mgdata_pgram_array_unnest_cluster_id_fork7_tbl AS
--   SELECT *
--   FROM mgdemo.mgdata_pgram_array_unnest_cluster_id_tbl
--   WHERE k = 7
-- DISTRIBUTED BY (k,cluster_id,bgid,point_id);
-- -- NOTICE:  table "mgdata_pgram_array_unnest_cluster_id_fork7_tbl" does not exist, skipping
-- -- Query returned successfully: 318240 rows affected, 1204 ms execution time.
--
--
-- DROP TABLE IF EXISTS mgdemo.mgdata_clean_with_id_and_cluster_id_fork7_tbl;
-- CREATE TABLE mgdemo.mgdata_clean_with_id_and_cluster_id_fork7_tbl AS
--   SELECT t2.k, t2.cluster_id, t1.*
--   FROM
--     mgdemo.mgdata_clean_with_id_tbl t1,
--     (SELECT k, cluster_id, bgid FROM mgdemo.mgdata_pgram_array_cluster_id_tbl WHERE k = 7 GROUP BY 1,2,3) t2
--   WHERE
--     t1.bgid = t2.bgid
-- DISTRIBUTED BY (k,cluster_id,bgid,rid);
-- -- NOTICE:  table "mgdata_clean_with_id_and_cluster_id_fork7_tbl" does not exist, skipping
-- -- Query returned successfully: 636480 rows affected, 18389 ms execution time.
