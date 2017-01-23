/*=================================================================================================
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

-- Function that loops through to run k-means clustering for different values of k
-- Compute SSE (sum of squared errors) and Silhouette coefficient for each k
CREATE OR REPLACE FUNCTION mgdemo.run_kmeans(
  input_table_with_schema VARCHAR,
  output_kmeans_table_with_schema VARCHAR,
  k_array INT[]
) RETURNS VOID AS
$$
  DECLARE
      sql TEXT;
      numk INT;
      silh_coef DOUBLE PRECISION;
      sse DOUBLE PRECISION;
      num_features INT;
  BEGIN
      sql := 'DROP TABLE IF EXISTS ' || output_kmeans_table_with_schema || ';';
      EXECUTE sql;
      sql := 'DROP TABLE IF EXISTS ' || output_kmeans_table_with_schema || '_with_clustid;';
      EXECUTE sql;
      sql := 'DROP TABLE IF EXISTS ' || output_kmeans_table_with_schema || '_silhcoef;';
      EXECUTE sql;
      sql := 'DROP TABLE IF EXISTS ' || output_kmeans_table_with_schema || '_unnest_cent_onelevel;';
      EXECUTE sql;
      sql := 'DROP TABLE IF EXISTS ' || output_kmeans_table_with_schema || '_sse;';
      EXECUTE sql;

      sql := 'CREATE TABLE ' || output_kmeans_table_with_schema
                || ' (k INT, centroids DOUBLE PRECISION[][], objective_fn DOUBLE PRECISION,'
                || ' frac_reassigned DOUBLE PRECISION, num_iterations INTEGER)'
                || ' DISTRIBUTED RANDOMLY;';
      EXECUTE sql;

      sql := 'CREATE TABLE ' || output_kmeans_table_with_schema || '_with_clustid'
                || ' (k INT, bgid INT, cluster_id INT)'
                || ' DISTRIBUTED RANDOMLY;';
      EXECUTE sql;

      sql := 'CREATE TABLE ' || output_kmeans_table_with_schema || '_silhcoef'
                || ' (k INT, silh_coef DOUBLE PRECISION)'
                || ' DISTRIBUTED RANDOMLY;';
      EXECUTE sql;

      sql := 'CREATE TABLE ' || output_kmeans_table_with_schema || '_unnest_cent_onelevel'
                || ' (k INT, cluster_id INT, element_order INT[], centroid DOUBLE PRECISION[])'
                || ' DISTRIBUTED RANDOMLY;';
      EXECUTE sql;

      sql := 'CREATE TABLE ' || output_kmeans_table_with_schema || '_sse'
                || ' (k INT, sse DOUBLE PRECISION)'
                || ' DISTRIBUTED RANDOMLY;';
      EXECUTE sql;

      numk := array_upper(k_array,1);
      sql := 'SELECT array_upper(pgram,1) FROM ' || input_table_with_schema;
      EXECUTE sql INTO num_features;

      FOR i IN 1..numk LOOP
          RAISE INFO '===== i = % ==== k = % ===== START =====', i, k_array[i];
          sql := 'INSERT INTO ' || output_kmeans_table_with_schema
                  || ' SELECT'
                  || ' ' || k_array[i] || ','
                  || ' *'
                  || ' FROM madlib.kmeanspp('
                  || ' '' ' ||  input_table_with_schema || ''','
                  || ' ''pgram'','
                  || ' ' || k_array[i] || ','
                  || ' ''madlib.dist_norm2'','
                  || ' ''madlib.avg'','
                  || ' 100, 0.001);'
                  ;
          RAISE INFO '===== query =====';
          RAISE INFO '%', sql;
          EXECUTE sql;

          RAISE INFO '===== assigning cluster IDs to data points =====';
          sql := 'INSERT INTO ' || output_kmeans_table_with_schema || '_with_clustid'
                  || ' SELECT k, bgid, (madlib.closest_column(centroids, pgram)).column_id AS cluster_id'
                  || ' FROM'
                  || '   ' || input_table_with_schema || ' AS data,'
                  || '   (SELECT * FROM ' || output_kmeans_table_with_schema
                  || '    WHERE k = ' || k_array[i] || ') AS kmeans_out';
          RAISE INFO '===== query =====';
          RAISE INFO '%', sql;
          EXECUTE sql;

          RAISE INFO '===== calculating silhouette_coefficeint =====';
          sql := 'SELECT madlib.simple_silhouette'
                        || ' (''' || input_table_with_schema || ''','
                        || ' ''pgram'','
                        || ' (SELECT centroids FROM ' || output_kmeans_table_with_schema || ' where k=' || k_array[i] || '),'
                        || ' ''madlib.dist_norm2'''
                        || ' );';
          RAISE INFO '===== query =====';
          RAISE INFO '%', sql;
          EXECUTE sql INTO silh_coef;
          RAISE INFO '===== silhouette_coefficeint=% =====', silh_coef;

          sql := 'INSERT INTO ' || output_kmeans_table_with_schema || '_silhcoef'
                  || ' VALUES (' || k_array[i] || ', ' || silh_coef || ');';
          RAISE INFO '===== query =====';
          RAISE INFO '%', sql;
          EXECUTE sql;

          RAISE INFO '===== START: unnest multidimensional centroids array by one level only =====';
          sql := 'INSERT INTO ' || output_kmeans_table_with_schema || '_unnest_cent_onelevel'
                  || ' SELECT k, cluster_id, array_agg(ucentorder ORDER BY ucentorder) AS element_order, array_agg(ucent ORDER BY ucentorder) AS centroid FROM'
                      || ' (SELECT t2.k, unnest(arr_ucid) AS cluster_id, unnest(arr_centorder) AS ucentorder, unnest(centroids) AS ucent FROM'
                          || ' (SELECT k, array_agg(ucidmult order by ucidmult,centorder) AS arr_ucid, array_agg(centorder ORDER BY ucidmult,centorder) AS arr_centorder FROM'
                              || ' (SELECT k, ucidmult, row_number() over (partition by k, ucidmult) AS centorder FROM'
                                  || ' (SELECT k, unnest(cidmult) AS ucidmult FROM'
                                      || ' (SELECT k, cid, madlib.array_fill(madlib.array_of_bigint(' || num_features || ')::int[],cid::int) AS cidmult FROM'
                                          || ' (SELECT k, generate_series(0,k-1,1) AS cid FROM (SELECT ' || k_array[i] || ' AS k) t0'
                                          || ' ) ta'
                                      || ' ) tb'
                                  || ' ) tc'
                              || ' ) td'
                          || ' GROUP BY k'
                          || ' ) t2,'
                          || ' (SELECT k, centroids FROM ' || output_kmeans_table_with_schema || ' WHERE k = ' || k_array[i]
                          || ' ) t3'
                      || ' WHERE t2.k = t3.k'
                      || ' ) t4'
                  || ' GROUP BY k, cluster_id;';
          RAISE INFO '%', sql;
          EXECUTE sql;
          RAISE INFO '===== END: unnest multidimensional centroids array by one level only =====';

          RAISE INFO '===== calculating sum of squared errors (sse) =====';
          sql := 'INSERT INTO ' || output_kmeans_table_with_schema || '_sse'
                  || ' SELECT k, sum(point_to_centroid_error) AS sse FROM'
                      || ' (SELECT clust_id_tbl.k AS k, clust_id_tbl.bgid, clust_id_tbl.cluster_id,'
                      || '   madlib.array_sum(madlib.array_square(madlib.array_sub(data.pgram,centroid))) AS point_to_centroid_error'
                      || ' FROM'
                      || ' ' || input_table_with_schema || ' AS data,'
                      || ' ' || output_kmeans_table_with_schema || '_with_clustid AS clust_id_tbl,'
                      || '   (SELECT * FROM ' || output_kmeans_table_with_schema || '_unnest_cent_onelevel'
                      || '    WHERE k = ' || k_array[i] || ') AS kmeans_cent_unnest_one_tbl'
                      || ' WHERE data.bgid = clust_id_tbl.bgid'
                      || '   AND clust_id_tbl.k = kmeans_cent_unnest_one_tbl.k'
                      || '   AND clust_id_tbl.cluster_id = kmeans_cent_unnest_one_tbl.cluster_id'
                      --|| ' GROUP BY clust_id_tbl.k, clust_id_tbl.bgid, clust_id_tbl.cluster_id'
                      || ' ) t1'
                  || ' GROUP BY k;';
          RAISE INFO '===== query =====';
          RAISE INFO '%', sql;
          EXECUTE sql;
          EXECUTE 'SELECT sse FROM ' || output_kmeans_table_with_schema || '_sse' || ' WHERE k = ' || k_array[i] || ';' INTO sse;
          RAISE INFO '===== sse=% =====', sse;

          RAISE INFO '===== i = % ==== k = % ===== END =====', i, k_array[i];
      END LOOP;
  END;
$$
LANGUAGE 'plpgsql';
--

-- Run the function for 3 to 20 clusters
SELECT mgdemo.run_kmeans('mgdemo.mgdata_pgram_array_tbl','mgdemo.mgdata_kmeans_out',array_agg(km order by km))
FROM (SELECT generate_series(3,20,1) as km) t1;
-- Total query runtime: 13873 ms.
-- 1 row retrieved.


/*
 * TABLE of centroids periodograms unnest from k-means output unnested. This TABLE is
 * used unnest the two dimensional centroids array from k-means output.
 */
CREATE TABLE mgdemo.mgdata_centroids_unnest_foreachk_tbl AS
  SELECT
    k,
    ((row_number() over(partition by k))+(array_len-1))/array_len AS array_id,
    index_id,
    cent_points
  FROM
  (
    SELECT
      k,
      array_len,
      generate_series(1,array_len,1) as index_id,
      unnest(centroids) AS cent_points
    FROM (
      SELECT k, centroids, array_upper(centroids,2) AS array_len
      FROM mgdemo.mgdata_kmeans_out
    ) t1
  ) t2
DISTRIBUTED BY (k,array_id,index_id);
-- Query returned successfully: 231840 rows affected, 3615 ms execution time.


-- TABLE of centroids AND respective cid's.
CREATE TABLE mgdemo.mgdata_centroids_cluster_id_tbl AS
  SELECT
    k,
    cent_coords,
    (madlib.closest_column(centroids, cent_coords)).column_id AS cluster_id
  FROM (
    SELECT
      t1.k,
      cent_coords,
      centroids
    FROM (
      SELECT
        k,
        array_agg(cent_points ORDER BY index_id) AS cent_coords
      FROM
        mgdemo.mgdata_centroids_unnest_foreachk_tbl
      GROUP BY
        k, array_id
    ) t1,
    mgdemo.mgdata_kmeans_out t2
    WHERE t1.k = t2.k
  ) t2
DISTRIBUTED RANDOMLY;
-- Query returned successfully: 322 rows affected, 292 ms execution time.


CREATE TABLE mgdemo.mgdata_pgram_datapnts_cluster_id_tbl AS
  SELECT
    k,
    bgid,
    pgram,
    (madlib.closest_column(centroids, pgram)).column_id AS cluster_id
  FROM
    mgdemo.mgdata_pgram_array_tbl,
    (SELECT k, centroids FROM mgdemo.mgdata_kmeans_out) t1
DISTRIBUTED BY (k,cluster_id,bgid)
;
-- Query returned successfully: 10166 rows affected, 8643 ms execution time.


-- TABLE of distances between pgrams and respective cluster centroids in LEVEL 1.0.
CREATE TABLE mgdemo.mgdata_pgramdata_centroid_dist_tbl AS
  SELECT
    t1.k,
    t2.cluster_id,
    bgid,
    sqrt(madlib.array_dot(madlib.array_sub(km_cent,km_pnts),madlib.array_sub(km_cent,km_pnts))) AS l2dist
  FROM (
    SELECT
      k,
      cluster_id,
      cent_coords::float8[] AS km_cent
    FROM
      mgdemo.mgdata_centroids_cluster_id_tbl
  ) t1,
  (
    SELECT
      k,
      cluster_id,
      bgid,
      pgram::float8[] AS km_pnts
    FROM
      mgdemo.mgdata_pgram_datapnts_cluster_id_tbl
  ) t2
  WHERE
    t1.k = t2.k
    AND
    t1.cluster_id = t2.cluster_id
DISTRIBUTED BY (k,cluster_id,bgid);


-- TABLE of meter readings AND respective LEVEL 1 cluster assignments.
-- Executing query:
CREATE TABLE mgdemo.mgdata_pgram_datapnts_unnest_cluster_id_tbl AS
  SELECT
    cid,
    unnest(coords) AS kmc,
    unnest(ridarray) AS rowid
  FROM (
    SELECT
      cid,
      pgram::float8[],
      ridarray
    FROM
      mgdemo.mgdata_pgram_datapnts_cluster_id_tbl t1,
      , ( SELECT
            array_agg(rid ORDER BY rid) AS ridarray
          FROM
            asdemo.as_genseries_1_to_3360
      ) t2
  ) t3
  DISTRIBUTED BY (cid);


-- Remove code below in later versions
-- TABLE of results from k-means algorithm.
-- drop table if exists mgdemo.mg_kmeans_k05_t1;
  CREATE TABLE mgdemo.mg_kmeans_k03_t1 AS
    SELECT *
    FROM
      madlib.kmeanspp(
        'mgdemo.mgdata_array_pgram',
        'pgram',
        3,
        'madlib.dist_norm2',
        'madlib.avg', 10, 0.001
      )
  ;
--


/*
-- template on how to unnest centroids table that is constructed in the plpgsql function
select k, ucid, array_agg(ucentorder order by ucentorder) as centorder, array_agg(ucent order by ucentorder) as cent from
(
select t2.k, unnest(arr_ucid) as ucid, unnest(arr_centorder) as ucentorder, unnest(cent) as ucent from
(
select k, array_agg(ucidmult order by ucidmult,centorder) as arr_ucid, array_agg(centorder order by ucidmult,centorder) as arr_centorder from
(
select k, ucidmult, row_number() over (partition by k, ucidmult) as centorder from
(
select k, unnest(cidmult) as ucidmult from
(
select k, cid, madlib.array_fill(madlib.array_of_bigint(2)::int[],cid::int) as cidmult from
(
select k, generate_series(0,k-1,1) as cid from (select generate_series(2,4,1) as k) t0
) ta
) tb
) tc
) td
group by k
--order by k
) t2,
(
-- replace with the centroids table from plpgsql output
select * from (select 2 as k, array[array[1,2],array[9,10]] as cent union all select 3, array[array[1,2],array[9,10],array[21,22]]
union all select 4, array[array[1,2],array[9,10],array[21,22],array[33,34]]) q3
) t3
where t2.k = t3.k
--order by t2.k, ucid, ucentorder
) t4
group by k, ucid
order by k, ucid;
*/



/*
 * TABLE of centroids periodograms unnest from k-means output unnested. This TABLE is
 * used unnest the two dimensional cetroids array from k-means output.
 */
CREATE TABLE mgdemo.centroids_unnest_k10 AS
  SELECT
    ((row_number() over())+1679)/1680 AS array_id
    , points
  FROM (
    SELECT
      unnest(centroids) AS points
    FROM
      asdemo.as_kmeans_level1
  ) t1;
