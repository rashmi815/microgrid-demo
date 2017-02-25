/*=================================================================================================
 *         APPLYING K-MEANS CLUSTERING
 *
 * - Cluster periodogram feature vectors using K-means algorithm in MADlib
 *    - Find the centroids of the clusters and cluster assignments for the periodograms
 *    - Re-cluster large clusters further
 *    - <Calculate silhouette co-effs and SSEs>
 *
 * -- Author: Rashmi Raghu
 *=================================================================================================
 */

 /*
 IMPORTANT NOTE:
 Re-running this code may alter results slightly as there is a random
 aspect to the k-means seeding process.
 If re-running this clustering code, make sure that the correct cluster is being
 re-clustered when multiple rounds of clustering are being done.
 If running more than 3 rounds of clustering, the post-processing code will also
 need to be modified to suit.
 */

---------------------------------------------------------------
-- Round 1 --
---------------------------------------------------------------
-- Call k-means clustering function in-database using MADlib
drop table if exists mgdemo.kmeans_output_tbl;
create table mgdemo.kmeans_output_tbl as
 select 10 as k, * from madlib.kmeanspp( 'mgdemo.mgdata_pgram_norm_array_tbl', 'pgram_norm_arr', 10, 'madlib.dist_norm2', 'madlib.avg', 100, 0.001)
distributed randomly;
-- Query returned successfully: one row affected, 6287 ms execution time.

-- Assign cluster IDs to all data points
drop table if exists mgdemo.mgdata_pgram_norm_array_cluster_id_tbl;
create table mgdemo.mgdata_pgram_norm_array_cluster_id_tbl as
 select
   k,
   (madlib.closest_column(centroids_multidim_array, pgram_norm_arr)).column_id as cluster_id,
   bgid,
   building_num,
   usagekw_sum_5min_mean,
   usagekw_sum_5min_norm_denom,
   win_id_arr,
   usagekw_sum_5min_arr,
   usagekw_sum_5min_norm_arr,
   pgram_pt_id_arr,
   pgram_norm_arr,
   win_id_arr as pgram_pt_id_arr_padded,
   pgram_norm_arr || madlib.array_of_float(array_upper(pgram_norm_arr,1)) as pgram_norm_arr_padded
 from
   mgdemo.mgdata_pgram_norm_array_tbl,
   (select k, centroids as centroids_multidim_array from mgdemo.kmeans_output_tbl) t1
distributed by (k,cluster_id,bgid);
-- Query returned successfully: 388 rows affected, 5708 ms execution time.

-- How many data points are in each cluster?
select cluster_id, count(*) from mgdemo.mgdata_pgram_norm_array_cluster_id_tbl
group by 1 order by 1;
-- 0;5
-- 1;1
-- 2;1
-- 3;1
-- 4;345
-- 5;1
-- 6;1
-- 7;1
-- 8;3
-- 9;29

---------------------------------------------------------------
-- Round 2 --
---------------------------------------------------------------
-- Cluster #4 from the above clustering result consists of over 88% of the data points
-- Cluster this set of data points further
drop table if exists mgdemo.mgdata_pgram_norm_array_r2_tbl;
create table mgdemo.mgdata_pgram_norm_array_r2_tbl as
 select
   k as k_r1,
   cluster_id as cluster_id_r1,
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
from mgdemo.mgdata_pgram_norm_array_cluster_id_tbl
where cluster_id = 4
distributed by (bgid);
-- Query returned successfully: 345 rows affected, 29696 ms execution time.

-- Call k-means clustering function in-database using MADlib
drop table if exists mgdemo.kmeans_output_r2_tbl;
create table mgdemo.kmeans_output_r2_tbl as
 SELECT 10 as k, * FROM madlib.kmeanspp( 'mgdemo.mgdata_pgram_norm_array_r2_tbl', 'pgram_norm_arr', 10, 'madlib.dist_norm2', 'madlib.avg', 100, 0.001)
distributed randomly;
-- Query returned successfully: one row affected, 6160 ms execution time.

-- Assign cluster IDs to data points
drop table if exists mgdemo.mgdata_pgram_norm_array_cluster_id_r2_tbl;
create table mgdemo.mgdata_pgram_norm_array_cluster_id_r2_tbl as
 select
   k,
   (madlib.closest_column(centroids_multidim_array, pgram_norm_arr)).column_id as cluster_id,
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
 from
   mgdemo.mgdata_pgram_norm_array_r2_tbl,
   (select k, centroids as centroids_multidim_array from mgdemo.kmeans_output_r2_tbl) t1
distributed by (k,cluster_id,bgid);
-- Query returned successfully: 345 rows affected, 8654 ms execution time.

-- How many data points are in each cluster?
select cluster_id, count(*) from mgdemo.mgdata_pgram_norm_array_cluster_id_r2_tbl
group by 1 order by 1;
-- 0;15
-- 1;2
-- 2;32
-- 3;1
-- 4;198
-- 5;62
-- 6;10
-- 7;4
-- 8;1
-- 9;20

---------------------------------------------------------------
-- Round 3 --
---------------------------------------------------------------
-- Cluster #4 above consists of over 57% of the data points that were input to the above clustering function
-- Cluster this set again
drop table if exists mgdemo.mgdata_pgram_norm_array_r3_tbl;
create table mgdemo.mgdata_pgram_norm_array_r3_tbl as
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
   pgram_norm_arr_padded
from mgdemo.mgdata_pgram_norm_array_cluster_id_r2_tbl
where cluster_id = 4
distributed by (bgid);
-- Query returned successfully: 198 rows affected, 3517 ms execution time.

-- Call k-means clustering function in-database using MADlib
drop table if exists mgdemo.kmeans_output_r3_tbl;
create table mgdemo.kmeans_output_r3_tbl as
 SELECT 10 as k, * FROM madlib.kmeanspp( 'mgdemo.mgdata_pgram_norm_array_r3_tbl', 'pgram_norm_arr', 10, 'madlib.dist_norm2', 'madlib.avg', 100, 0.001)
distributed randomly;
-- Query returned successfully: one row affected, 16586 ms execution time.

-- Assign cluster IDs to data points
drop table if exists mgdemo.mgdata_pgram_norm_array_cluster_id_r3_tbl;
create table mgdemo.mgdata_pgram_norm_array_cluster_id_r3_tbl as
 select
   k,
   (madlib.closest_column(centroids_multidim_array, pgram_norm_arr)).column_id as cluster_id,
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
 from
   mgdemo.mgdata_pgram_norm_array_r3_tbl,
   (select k, centroids as centroids_multidim_array from mgdemo.kmeans_output_r3_tbl) t1
distributed by (k,cluster_id,bgid);
-- Query returned successfully: 198 rows affected, 1689 ms execution time.

-- How many data points are in each cluster?
select cluster_id, count(*) from mgdemo.mgdata_pgram_norm_array_cluster_id_r3_tbl
group by 1 order by 1;
-- 0;98
-- 1;7
-- 2;11
-- 3;1
-- 4;6
-- 5;28
-- 6;1
-- 7;1
-- 8;34
-- 9;11
