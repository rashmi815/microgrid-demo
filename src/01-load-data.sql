/*=================================================================================================
 *		LOADING DATA
 *
 * - Load the single csv file into GPDB or HDB
 * - Data: Microgrid dataset sourced from the UMASS Trace Repository: http://traces.cs.umass.edu/index.php/Smart/Smart
 * ---- Contains electrical data over a single 24-hour period from 443 unique homes
 * ---- Original dataset from website contains 443 csv files - one for each home
 * ---- The home/building number in original dataset is given as part of file name
 * ---- Used Python notebook to read in and create single file from the 443 files
 * ---- Appended the building number when creating single file
 * ---- Format of single data file: Building Number (integer), Local Timestamp (long/bigint), Usage kW (double precision)
 * ---- NOTE: Unzip the data file: data/microgrid_all.csv.gz before running this script
 *
 *=================================================================================================
 */

-- Create schema for demo
create schema mgdemo;
-- Query returned successfully with no result in 45 ms.

-- Create empty table
create table mgdemo.microgrid_data (building_num int, tslocal bigint, usagekw double precision)
  distributed by (building_num);
-- Query returned successfully with no result in 30 ms.

-- Copy data from file into table
-- Unzip the data file before running the query below
-- Make sure to replace the file name and path in the query below with the right one for your system
copy mgdemo.microgrid_data from 'microgrid_all.csv' delimiter ',' csv;
-- COPY 637526
-- Time: 1443.865 ms
