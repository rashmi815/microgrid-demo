/*=================================================================================================
 *				 GENERATING PERIODOGRAMS
 *
 * - Aggregate the remaining time series data for each smart meter into arrays
 * - Use spec.pgram function in PL/R to compute periodograms
 *
 * -- Author: Rashmi Raghu
 *=================================================================================================
 */

-- PL/R function that computes the periodogram based on Fast Fourier Transform algorithm
CREATE OR REPLACE FUNCTION mgdemo.pgram_fn(tsval double precision[], taperval double precision)
  RETURNS double precision[] AS
$$
  #spec.pgram function options:
  #taper default 0.1 -- recommend 0.0 input for this use case
  #detrend set to FALSE - mean value & trend will NOT be removed from signal prior to transform
  rpgram <- spec.pgram(tsval,fast=FALSE,plot=FALSE,taper=taperval,detrend=FALSE)
  rpout <- rpgram$spec
  return(rpout)
$$
LANGUAGE 'plr';
-- Query returned successfully with no result in 49 ms.

-- Exclude points that have nearly zero (but not quite zero) signal values
drop table if exists mgdemo.mgdata_clean_with_id_nozero_sum_5min_norm_array_tbl;
create table mgdemo.mgdata_clean_with_id_nozero_sum_5min_norm_array_tbl as
  select *
  from
  (
    select
      bgid,
      building_num,
      usagekw_sum_5min_mean,
      usagekw_sum_5min_norm_denom,
      array_agg(win_id order by win_id) as win_id_arr,
      array_agg(usagekw_sum_5min order by win_id) as usagekw_sum_5min_arr,
      array_agg(usagekw_sum_5min_norm order by win_id) as usagekw_sum_5min_norm_arr
    from
      mgdemo.mgdata_clean_with_id_nozero_sum_5min_norm_tbl
    group by 1,2,3,4
  ) t1
  where usagekw_sum_5min_norm_denom > 1e-10 -- exclude points that have nearly zero values
DISTRIBUTED by (bgid);
-- Query returned successfully: 388 rows affected, 13303 ms execution time.

-- Generate the periodogram features
drop table if exists mgdemo.mgdata_pgram_norm_array_tbl;
create table mgdemo.mgdata_pgram_norm_array_tbl as
  select
    bgid,
    building_num,
    usagekw_sum_5min_mean,
    usagekw_sum_5min_norm_denom,
    win_id_arr,
    usagekw_sum_5min_arr,
    usagekw_sum_5min_norm_arr,
    win_id_arr[1:(array_upper(win_id_arr,1)/2)] as pgram_pt_id_arr,
    mgdemo.pgram_fn(usagekw_sum_5min_norm_arr,0.0) as pgram_norm_arr
  from
    mgdemo.mgdata_clean_with_id_nozero_sum_5min_norm_array_tbl
distributed by (bgid);
-- NOTICE:  table "mgdata_pgram_norm_array_tbl" does not exist, skipping
-- Query returned successfully: 388 rows affected, 32609 ms execution time.
