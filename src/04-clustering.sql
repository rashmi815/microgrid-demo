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

--===================
-- LEVEL 1.0 Clustering
--===================

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
