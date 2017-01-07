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
 * ---- Format of single data file: Building Number, Local Timestamp, Usage kW
 * ---- NOTE: Unzip the data file: data/microgrid_all.csv.gz before running this script
 *
 *=================================================================================================
 */

-- Create schema for demo
  create schema mgd;

-- Create empty table
  create table mgd.microgrid_data (building_num int, tslocal bigint, usagekw double precision);

-- Copy data from file into table
-- Unzip the data file before running th
-- Make sure to replace the file name and path in the query below with the right one for your system
copy mgd.microgrid_data from './data/microgrid_all.csv' delimiter ',' csv;
