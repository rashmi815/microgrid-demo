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
  BEGIN
      sql := 'DROP TABLE IF EXISTS ' || output_kmeans_table_with_schema || ';';
      EXECUTE sql;

      sql := 'CREATE TABLE ' || output_kmeans_table_with_schema
                || ' (centroids DOUBLE PRECISION[][], objective_fn DOUBLE PRECISION,'
                || ' frac_reassigned DOUBLE PRECISION, num_iterations INTEGER)'
                || ' DISTRIBUTED RANDOMLY;';
      EXECUTE sql;

      numk := array_upper(k_array,1);

      FOR i IN 1..numk LOOP
          RAISE INFO '===== i = % ==== k = % ===== START ====', i, k_array[i];
          sql := 'INSERT INTO ' || output_kmeans_table_with_schema
                  || ' SELECT * '
                  || ' FROM madlib.kmeanspp('
                  || ' '' ' ||  input_table_with_schema || ''','
                  || ' ''pgram'','
                  || ' ' || k_array[i] || ','
                  || ' ''madlib.dist_norm2'','
                  || ' ''madlib.avg'','
                  || ' 100, 0.001);'
                  ;
          EXECUTE sql;
          RAISE INFO '===== i = % ==== k = % ===== END ====', i, k_array[i];
      END LOOP;
  END;
$$
LANGUAGE 'plpgsql';
-- Query returned successfully with no result in 33 ms.

-- Run the function for 3 to 20 clusters
SELECT mgdemo.run_kmeans('mgdemo.mgdata_pgram_array_tbl','mgdemo.mgdata_kmeans_out',array_agg(km order by km))
FROM (SELECT generate_series(3,20,1) as km) t1;
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
