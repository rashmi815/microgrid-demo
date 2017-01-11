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
CREATE TABLE mgdemo.mgdata_array AS
  SELECT
  building_num
  , tslocal_array[1] AS tslocal_first
  , tslocal_array[1440] AS tslocal_last
  , array_upper(tslocal_array[1:1440],1) AS array_length
  , tslocal_array
  , usagekw_array
  FROM (
    SELECT
      building_num
      , array_agg(tslocal ORDER BY tslocal) AS tslocal_array
      , array_agg(usagekw ORDER BY tslocal) AS usagekw_array
    FROM
    mgdemo.mgdata_dfct1_itvlct1440_t1
    GROUP BY
    building_num
  ) t1
DISTRIBUTED BY (building_num);
-- Query returned successfully: 442 rows affected, 1297 ms execution time.

-- PL/R function that computes the periodogram based on Fast Fourier Transform algorithm
CREATE OR REPLACE FUNCTION mgdemo.pgram_fn(tsval double precision[], taperval double precision)
  RETURNS double precision[] AS
$BODY$
  #spec.pgram function options:
  #taper changed to 0.0 from default 0.1
  #demean set to TRUE - mean value will be removed from signal
  rpgram <- spec.pgram(tsval,fast=FALSE,plot=FALSE,taper=taperval,demean=TRUE,detrend=FALSE)
  rpout <- rpgram$spec
  return(rpout)
$BODY$
  LANGUAGE 'plr' VOLATILE;
-- -- Query returned successfully with no result in 30 ms.

-- TABLE of periodogram arrays for each device
CREATE SEQUENCE 'gen_ids';
-- Query returned successfully with no result in 31 ms.

SELECT setval('gen_ids',1); -- 0 is not accepted by the sequence
CREATE TABLE mgdemo.mgdata_array_pgram AS
  SELECT
    building_num
    , nextval('gen_ids') AS rid
    , pgram
  FROM (
    SELECT
      building_num
      , mgdemo.pgram_fn(usagekw_array,0.0) AS pgram
    FROM
      mgdemo.mgdata_array
  ) t1
DISTRIBUTED BY (building_num);
-- query result with 1 row discarded.
-- Query returned successfully: 442 rows affected, 564 ms execution time.


-- TABLE of periodograms unnested for each device
CREATE TABLE mgdemo.mgdata_array_pgram_unnest AS
  SELECT
    building_num
    , rid
    , generate_series(1,array_upper(pgram,1)) as pgid
    , unnest(pgram) as pgram_val
  FROM
    mgdemo.mgdata_array_pgram
DISTRIBUTED BY (rid,pgid);
-- Query returned successfully: 318240 rows affected, 851 ms execution time.
