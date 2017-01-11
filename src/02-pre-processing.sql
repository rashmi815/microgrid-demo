/*=================================================================================================
 *		PRE-PROCESSING
 *
 * - Check that the data points are contiguous in time without breaks
 * - Exclude those smart meters that do not have data over the complete time window desired
 *
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

Time: 89.946 ms
*/

/*
 * VIEW of all the meters which have a delta_time of 60 seconds, ignoring others.
 * Only data collected every 60 seconds will be used.
 * delta_time represents the time between two consecutive timestamps of a meter.
 */
CREATE OR REPLACE VIEW mgdemo.mgdata_dt60sec_check_view1 AS
	SELECT *
	, ct_tslocal-ct_deltatime_60sec AS diff_ct
	FROM (
		SELECT
			building_num
			, COUNT(tslocal) AS ct_tslocal
			, COUNT(difftime) FILTER (WHERE difftime = 60::int) AS ct_deltatime_60sec
		FROM (
			SELECT
				building_num
				, tslocal
				, tslocal-LAG(tslocal,1,tslocal) OVER (PARTITION BY building_num ORDER BY tslocal) AS difftime
			FROM
				mgdemo.microgrid_data
		) t1
		GROUP BY
			building_num
	) t2;
-- CREATE VIEW
-- Time: 19.838 ms

select * from mgdemo.mgdata_dt60sec_check_view1 order by building_num;
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
Time: 803.451 ms
*/

-- Count how many buildings have different values for diff_ct
-- diff_ct should equal 1 for time series with data at 60 second intervals
select diff_ct, count(*) as ct_buildings
from mgdemo.mgdata_dt60sec_check_view1
group by diff_ct
order by diff_ct;
/*
diff_ct | ct_buildings
---------+--------------
			1 |          443
(1 row)

Time: 1059.357 ms
*/

/*
 * VIEW with only those meters that have diff_ct = 1. This query gets those meters
 * consistently collected data every 60 seconds.
 */
CREATE OR REPLACE VIEW mgdemo.mgdata_diffct1_view1 AS
SELECT t1.*
FROM
	mgdemo.microgrid_data t1
	, mgdemo.mgdata_dt60sec_check_view1 t2
WHERE
	t1.building_num = t2.building_num
	AND t2.diff_ct = 1;
-- CREATE VIEW
-- Time: 20.527 ms

-- Note the number of buildings that have diff_ct =1 should match the result
-- in the query above that counts how many buildings have different diff_ct values
select count(*) as ct_buildings
from (
	select building_num from mgdemo.mgdata_diffct1_view1
	group by 1
) t1;
-- ct_buildings
-- --------------
-- 				 443
-- (1 row)
--
-- Time: 1284.810 ms

-- TABLE containing start and end times for each meter and COUNT of how many data points we have.
CREATE TABLE mgdemo.mgdata_dfct1_tslocalcts_t1 AS
	SELECT *
	FROM (
		SELECT
			building_num,
			min(tslocal) AS ts_start,
			max(tslocal) AS ts_end,
			COUNT(tslocal) AS ct_tslocal
		FROM
			mgdemo.mgdata_diffct1_view1
		GROUP BY
			building_num
		) t1
		DISTRIBUTED BY (building_num);
-- SELECT 443
-- Time: 860.186 ms

-- Check how many meters have different start and end points
-- or if the number of points included in between is different
-- It is ideal to have the same start and end points,
-- and the same number of data points in between
select ts_start, ts_end, ct_tslocal, count(*) as ct
from mgdemo.mgdata_dfct1_tslocalcts_t1
group by 1,2,3
order by 1,2,3;
/*
ts_start  |   ts_end   | ct_tslocal | ct
------------+------------+------------+-----
1301702400 | 1301788740 |       1440 | 442
1301726040 | 1301788740 |       1046 |   1
(2 rows)

Time: 28.633 ms
*/

/*
 * TABLE making sure COUNTs of data points are correct.
 * Each device should have the same start time and end time
 * and interval COUNT of 1440 (the number of minutes in a 24-hour period).
 * TABLE to be used as starting point for clustering.
 */
CREATE TABLE mgdemo.mgdata_dfct1_itvlct1440_t1 AS
	SELECT t1.*
	FROM
		mgdemo.mgdata_diffct1_view1 t1
		, mgdemo.mgdata_dfct1_tslocalcts_t1 t2
	WHERE
		t1.building_num = t2.building_num
		AND t2.ct_tslocal = 1440
DISTRIBUTED BY (building_num);
-- SELECT 636480
-- Time: 994.049 ms

-- This query should show only one ct_ts value (1440 for this dataset)
-- and the right number of buildings associated with it (442 for this dataset)
SELECT ct_ts, count(*) as ct_buildings from (
	select building_num, count(*) as ct_ts from mgdemo.mgdata_dfct1_itvlct1440_t1
	group by 1
) t1
group by 1
order by 1;
-- ct_ts | ct_buildings 
-- -------+--------------
--  1440 |          442
-- (1 row)
--
-- Time: 140.794 ms
