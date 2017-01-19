/*=================================================================================================
 *				 APPLYING K-MEANS CLUSTERING
 *
 * - Cluster periodogram feature vectors using K-means algorithm in MADlib using k=...
 * 		- Find the centroids of the clusters, cluster assignments for the periodograms, and the
 *			distances between each point and centroid for each cluster
 *		- Calculate silhouette coefficients for every data point(pgram) in each cluster level.
 * 		- Calculate the average silhouette coefficient for each cluster level.
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
  BEGIN
      sql := 'DROP TABLE IF EXISTS ' || output_kmeans_table_with_schema || ';';
      EXECUTE sql;
      sql := 'DROP TABLE IF EXISTS ' || output_kmeans_table_with_schema || '_with_clustid;';
      EXECUTE sql;
      sql := 'DROP TABLE IF EXISTS ' || output_kmeans_table_with_schema || '_silhcoef;';
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

      sql := 'CREATE TABLE ' || output_kmeans_table_with_schema || '_sse'
                || ' (k INT, sse DOUBLE PRECISION)'
                || ' DISTRIBUTED RANDOMLY;';
      EXECUTE sql;

      numk := array_upper(k_array,1);

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

          -- RAISE INFO '===== calculating sum of squared errors (sse) =====';
          -- sql := 'INSERT INTO ' || output_kmeans_table_with_schema || '_sse'
          --         || ' SELECT k, bgid, data.pgram - '
          --         || ' FROM'
          --         || ' ' || input_table_with_schema || ' as data'
          --         || ' ' || output_kmeans_table_with_schema || '_with_clustid as clust_id_tbl,'
          --         || '   (SELECT * FROM ' || output_kmeans_table_with_schema
          --         || '    WHERE k = ' || k_array[i] || ') AS kmeans_out'
          --         || ' WHERE data.bgid = clust_id_tbl.bigid'
          --         || ' GROUP BY cluster_id';
          -- RAISE INFO '===== query =====';
          -- RAISE INFO '%', sql;
          -- EXECUTE sql INTO sse;
          -- RAISE INFO '===== sse=% =====', sse;

          RAISE INFO '===== i = % ==== k = % ===== END =====', i, k_array[i];
      END LOOP;
  END;
$$
LANGUAGE 'plpgsql';
--

-- Run the function for 3 to 20 clusters
SELECT mgdemo.run_kmeans('mgdemo.mgdata_pgram_array_tbl','mgdemo.mgdata_kmeans_out',array_agg(km order by km))
FROM (SELECT generate_series(3,5,1) as km) t1;
-- Total query runtime: 13873 ms.
-- 1 row retrieved.

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
