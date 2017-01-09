/*=================================================================================================
 *		PRE-PROCESSING
 *
 * - Check that the data points are contiguous in time without breaks
 * - Exclude those smart meters that do not have data over the complete time window desired
 *
 *=================================================================================================
 */

-- Sample rows of data
select * from mgdemo.microgrid_data order by buidling_num, tslocal limit 10;

/*
 * VIEW of all the meters which have a delta_time of 60 seconds, ignoring others.
 * Only data collected every 60 seconds will be used.
 * delta_time represents the time between two consecutive timestamps of a meter.
 */
-- Executing query:
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

-- select * from mgdemo.mgdata_dt30min_check_view1 order by building_num;

select diff_ct, count(*) as ct_buildings
from mgdemo.mgdata_dt60sec_check_view1
group by diff_ct
order by diff_ct;

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

select count(*) as ct_buildings
from
(
	select building_num from mgdemo.mgdata_diffct1_view1
	group by 1
) t1;


-- TABLE containing start and end times for each meter and COUNT of how many data points we have.
CREATE TABLE mgdemo.mgdata_dfct1_itvlcts_t1 AS
	SELECT *
	FROM (
		SELECT
			building_num,
			min(tslocal) AS ts_start,
			max(tslocal) AS ts_end,
			COUNT(tslocal) AS ct_itvl_ts
		FROM
			mgdemo.mgdata_diffct1_view1
		GROUP BY
			building_num
		) t1
		DISTRIBUTED BY (building_num);
