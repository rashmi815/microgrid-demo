/*=================================================================================================
 *				 GENERATING PERIODOGRAMS
 *
 * - Aggregate the remaining time series data for each smart meter into arrays
 *		- Remaining time series data (TABLE mgdemo.mgdata_dfct1_itvlct1440_t1) is for 24 hours of data
 *			(1440 readings per meter)
 * - Use spec.pgram function in PL/R to compute periodograms
 *=================================================================================================
 */

-- TABLE of 1 day of time points and usagekw readings aggregated into arrays for each device
CREATE TABLE mgdemo.mgdata_ts_array_tbl AS
  SELECT
    bgid,
    tslocal_array,
    usagekw_array,
    rgid_first_last,
    rid_first_last,
    ARRAY[tslocal_array[1],tslocal_array[1440]] AS tslocal_first_last,
    array_upper(tslocal_array[1:1440],1) AS array_length,
    building_num
  FROM (
    SELECT
      ARRAY[MIN(rgid), MAX(rgid)] AS rgid_first_last,
      bgid,
      ARRAY[MIN(rid), MAX(rid)] AS rid_first_last,
      building_num,
      array_agg(tslocal ORDER BY tslocal) AS tslocal_array,
      array_agg(usagekw ORDER BY tslocal) AS usagekw_array
    FROM
      mgdemo.mgdata_clean_with_id_tbl
    GROUP BY
      building_num, bgid
  ) t1
DISTRIBUTED BY (bgid);
-- Query returned successfully: 442 rows affected, 1099 ms execution time.

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

-- TABLE of periodogram arrays for each device
CREATE SEQUENCE gen_ids;
-- Query returned successfully with no result in 34 ms.

SELECT setval('gen_ids',1); -- 0 is not accepted by the sequence
CREATE TABLE mgdemo.mgdata_pgram_array_tbl AS
  SELECT
    bgid,
    mgdemo.pgram_fn(usagekw_array,0.0) AS pgram
  FROM
    mgdemo.mgdata_ts_array_tbl
DISTRIBUTED BY (bgid);
-- query result with 1 row discarded.
-- Query returned successfully: 442 rows affected, 464 ms execution time.


-- TABLE of periodograms unnested for each device
CREATE TABLE mgdemo.mgdata_pgram_array_unnest_tbl AS
  SELECT
    bgid,
    generate_series(1,array_upper(pgram,1)) as pgid,
    unnest(pgram) as pgram_val
  FROM
    mgdemo.mgdata_pgram_array_tbl
DISTRIBUTED BY (bgid,pgid);
-- Query returned successfully: 318240 rows affected, 529 ms execution time.
