/*=================================================================================================
 *		PRE-PROCESSING
 *
 * - Check that the data points are contiguous in time without breaks
 * - Exclude those smart meters that do not have data over the complete time window desired
 * - Remaining time series data is for 1440 readings per signal covering a 24-hour period
 *    - aggregated every 5-minutes and normalized (288 readings per building)
 *
 * -- Author: Rashmi Raghu
 *=================================================================================================
 */

-- Sample rows of data
select * from mgdemo.microgrid_data order by building_num, tslocal limit 10;
/*
building_num |  tslocal   |   usagekw
--------------+------------+-------------
           6 | 1301702400 | 5.166266667
           6 | 1301702460 | 5.136533333
           6 | 1301702520 | 4.995633333
           6 | 1301702580 |      4.9808
           6 | 1301702640 |       4.987
           6 | 1301702700 | 5.148833333
           6 | 1301702760 |      5.3055
           6 | 1301702820 | 6.387166667
           6 | 1301702880 | 5.462366667
           6 | 1301702940 | 4.117166667
(10 rows)

Time: 60.157 ms
*/

/*
 * VIEW of all the meters which have a delta_time of 60 seconds, ignoring others.
 * Only data collected every 60 seconds will be used.
 * delta_time represents the time between two consecutive timestamps of a meter.
 */
create or replace view mgdemo.mgdata_deltatime_60sec_check_view as
  select
    *,
    ct_tslocal-ct_deltatime_60sec as diff_ct
  from (
    select
      building_num,
      count(tslocal) as ct_tslocal,
      count(difftime) filter (where difftime = 60::int) as ct_deltatime_60sec
    from (
      select
        building_num,
        tslocal,
        tslocal-lag(tslocal,1,tslocal) over (partition by building_num order by tslocal) as difftime
      from
        mgdemo.microgrid_data
    ) t1
    group by
      building_num
  ) t2;
-- Query returned successfully with no result in 50 ms.

select * from mgdemo.mgdata_deltatime_60sec_check_view order by building_num;
/*
building_num | ct_tslocal | ct_deltatime_60sec | diff_ct
--------------+------------+--------------------+---------
           6 |       1440 |               1439 |       1
           7 |       1440 |               1439 |       1
          15 |       1440 |               1439 |       1
          16 |       1440 |               1439 |       1
          21 |       1440 |               1439 |       1
          24 |       1440 |               1439 |       1
          34 |       1440 |               1439 |       1
          35 |       1440 |               1439 |       1
          36 |       1440 |               1439 |       1
...
*/

-- Count how many buildings have different values for diff_ct
-- diff_ct should equal 1 for time series with data at 60 second intervals
select diff_ct, count(*) as ct_buildings
from mgdemo.mgdata_deltatime_60sec_check_view
group by diff_ct
order by diff_ct;
/*
diff_ct | ct_buildings
---------+--------------
      1 |          443
(1 row)

Time: 1079.926 ms
*/

/*
 * VIEW with only those meters that have diff_ct = 1. This query gets those meters
 * consistently collected data every 60 seconds.
 */
create or replace view mgdemo.mgdata_counts_check_join_view as
select t1.*
from
  mgdemo.microgrid_data t1,
  mgdemo.mgdata_deltatime_60sec_check_view t2
where
  t1.building_num = t2.building_num
  and
  t2.diff_ct = 1
;
-- Query returned successfully with no result in 36 ms.

-- Note the number of buildings that have diff_ct =1 should match the result
-- in the query above that counts how many buildings have different diff_ct values
select count(*) as ct_buildings
from (
  select building_num
  from mgdemo.mgdata_counts_check_join_view
  group by 1
) t1;
-- ct_buildings
-- --------------
--          443
-- (1 row)
--
-- Time: 1190.595 ms


-- TABLE containing start and end times for each meter and COUNT of how many data points we have.
create table mgdemo.mgdata_counts_and_startend_times_tbl as
  select *
  from (
    select
      building_num,
      min(tslocal) as ts_start,
      max(tslocal) as ts_end,
      count(tslocal) as ct_tslocal
    from
      mgdemo.mgdata_counts_check_join_view
    group by
      building_num
  ) t1
distributed by (building_num);
-- Query returned successfully: 443 rows affected, 1237 ms execution time.

-- Check how many meters have different start and end points
-- or if the number of points included in between is different
-- It is ideal to have the same start and end points,
-- and the same number of data points in between
select ts_start, ts_end, ct_tslocal, count(*) as ct
from mgdemo.mgdata_counts_and_startend_times_tbl
group by 1,2,3
order by 1,2,3;
/*
ts_start  |   ts_end   | ct_tslocal | ct
------------+------------+------------+-----
1301702400 | 1301788740 |       1440 | 442
1301726040 | 1301788740 |       1046 |   1
(2 rows)

Time: 35.755 ms
*/

/*
 * TABLE making sure COUNTs of data points are correct.
 * Each device should have the same start time and end time
 * and interval COUNT of 1440 (the number of minutes in a 24-hour period).
 * TABLE to be used as starting point for clustering.
 */
create table mgdemo.mgdata_clean_tbl as
  select t1.*
  from
    mgdemo.mgdata_counts_check_join_view t1,
    mgdemo.mgdata_counts_and_startend_times_tbl t2
  where
    t1.building_num = t2.building_num
  and t2.ct_tslocal = 1440
distributed by (building_num);
-- Query returned successfully: 636480 rows affected, 2093 ms execution time.

-- This query should show only one ct_ts value (1440 for this dataset)
-- and the right number of buildings associated with it (442 for this dataset)
select ct_ts, count(*) as ct_buildings from (
  select building_num, count(*) as ct_ts from mgdemo.mgdata_clean_tbl
  group by 1
) t1
group by 1
order by 1;
-- ct_ts | ct_buildings
-- -------+--------------
--  1440 |          442
-- (1 row)
--
-- Time: 170.593 ms

/*
Create table with contiguous building id, global row id, building-specific row id
for time series
*/
create table mgdemo.mgdata_clean_with_id_tbl as
  select
    rgid,
    bgid,
    rid,
    t1.building_num,
    tslocal,
    usagekw
  from (
    select
      row_number() over (order by building_num, tslocal) as rgid,
      row_number() over (partition by building_num order by tslocal) as rid,
      *
    from
      mgdemo.mgdata_clean_tbl
  ) t1,
  (
    select
      row_number() over (order by building_num) as bgid,
      building_num
    from (
      select building_num
      from mgdemo.mgdata_clean_tbl
      group by building_num
    ) t2
  ) t3
  where t1.building_num = t3.building_num
distributed by (rgid);
-- Query returned successfully: 636480 rows affected, 2022 ms execution time.


/*
After further exploration of the data (including preliminary clustering results),
remove signals with all zeros from the dataset for more meaningful results
*/
drop table if exists mgdemo.mgdata_clean_with_id_nozero_tbl;
create table mgdemo.mgdata_clean_with_id_nozero_tbl as
  select
    rgid,
    t1.bgid,
    rid,
    building_num,
    tslocal,
    (tslocal - min(tslocal) over (partition by t1.bgid)) as tslocal_fromzero,
    usagekw
  from
   mgdemo.mgdata_clean_with_id_tbl t1,
   (
     select bgid from
     (
       select bgid, min(usagekw) as minu, max(usagekw) as maxu
       from mgdemo.mgdata_clean_with_id_tbl
       group by 1
     ) t2
     where minu <> 0.0 or maxu <> 0.0
   ) t3
  where t1.bgid = t3.bgid
distributed by (bgid,rid);
-- Query returned successfully: 568800 rows affected, 64466 ms execution time.


/*
Aggregate signals over 5-minute time windows to smooth out very high frequencies
that might be adding too much noise to the data
*/
drop table if exists mgdemo.mgdata_clean_with_id_nozero_sum_5min_tbl;
create table mgdemo.mgdata_clean_with_id_nozero_sum_5min_tbl as
  select
    bgid,
    win_id,
    sum(usagekw) as usagekw_sum_5min,
    building_num,
    array_agg(rgid order by rid) as rgid_arr,
    array_agg(rid order by rid) as rid_arr,
    array_agg(tslocal order by rid) as tslocal_arr,
    array_agg(tslocal_fromzero order by rid) as tslocal_fromzero_arr,
    array_agg(usagekw order by rid) as usagekw_arr
  from
  (
    select *,
      tslocal_fromzero::int / 300 as win_id
    from
      mgdemo.mgdata_clean_with_id_nozero_tbl
  ) t1
  group by bgid, win_id, building_num
DISTRIBUTED by (bgid,win_id);
-- Query returned successfully: 113760 rows affected, 28592 ms execution time.


/*
Normalize the aggregated / smoothed signals
This will be used as input to periodogram generation and clustering following that
*/
drop table if exists mgdemo.mgdata_clean_with_id_nozero_sum_5min_norm_tbl;
create table mgdemo.mgdata_clean_with_id_nozero_sum_5min_norm_tbl as
  select
    bgid,
    win_id,
    usagekw_sum_5min,
    (usagekw_sum_5min - usagekw_sum_5min_mean) / usagekw_sum_5min_norm_denom as usagekw_sum_5min_norm,
    usagekw_sum_5min_mean,
    usagekw_sum_5min_norm_denom,
    building_num,
    rgid_arr,
    rid_arr,
    tslocal_arr,
    tslocal_fromzero_arr,
    usagekw_arr
  from
  (
    select
      sum((usagekw_sum_5min - usagekw_sum_5min_mean)^2) over (partition by bgid) as usagekw_sum_5min_norm_denom,
      *
    from
    (
      select
        avg(usagekw_sum_5min) over (partition by bgid) as usagekw_sum_5min_mean,
        *
      from
        mgdemo.mgdata_clean_with_id_nozero_sum_5min_tbl
    ) t1
  ) t2
DISTRIBUTED by (bgid,win_id);
-- NOTICE:  table "mgdata_clean_with_id_nozero_sum_5min_norm_tbl" does not exist, skipping
-- Query returned successfully: 113760 rows affected, 15699 ms execution time.
