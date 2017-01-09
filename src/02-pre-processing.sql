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
CREATE OR REPLACE VIEW mgdemo.mgdata_dt30min_check_view1 AS
	SELECT *
	, ct_tslocal-cnt_deltatime_60sec AS diff_ct
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

select * from mgdemo.mgdata_dt30min_check_view1 order by building_num;
