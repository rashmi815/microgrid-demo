# microgrid-demo

This demo shows how one can do load profiling using smart meter power usage data. Load profiling is about understanding patterns of usage over relevant periods of time (such as day/week etc). Different users will exhibit different usage patterns and clustering them provides an understanding load profiles of various groups. Understanding the usage behavior of different groups of consumers will enable better planning for current and future demand. 

This analysis uses the Microgrid [dataset] (http://smart.cs.umass.edu/download.php?t=microgrid) from the [UMASS Trace repository] (http://traces.cs.umass.edu/index.php/Smart/Smart)

The analysis presented in this repo has also been successfully applied to commercial smart meter data and provided useful insights by clustering similar consumers together based only on their power usage signals.


## Demo the results (results packaged with the dashboard)
###### Requires:
* Tableau installed on the machine from which you will view the dashboard (e.g. your laptop)
* Make sure the version of Tableau installed can open the file

###### Instructions:
* Download the latest Tableau workbook (with extract) file that has the .twbx extension from the twbx/ directory
* Open the file and click on the Dashboard tab
* Dashboard has several panels whose output will be described below


## Demo the results by live-connecting to the database
###### Requires:
* Tableau installed on the machine from which you will view the dashboard (e.g. your laptop)
    * Make sure the version of Tableau installed can open the file
* GPDB or Postgres installation (on laptop /VM / in the cloud / on-premise) with the required results tables
    * IP address to device hosting GPDB or Postgres and ability to successfully connect to that IP
    * Login credentials to the GPDB or Postgres database

###### Instructions:
* Download the latest Tableau workbook (without extract) file that has the .twb extension from the twb/ directory
* Open the file and click on any sheet
* Open the file and click on the Dashboard tab
* Dashboard has several panels whose output will be described below


## Demo the code by running it and the results by live-connecting to the database 
###### Requires:
* Tableau installed on the machine from which you will view the dashboard (e.g. your laptop)
    * Make sure the version of Tableau installed can open the file
* GPDB or Postgres installation (on laptop /VM / in the cloud / on-premise) with the required results tables
    * IP address to device hosting GPDB or Postgres and ability to successfully connect to that IP
    * Login credentials to the GPDB or Postgres database
    * MADlib installed (the demo runs the 'k-means' function)
    * PL/R installed (the demo uses the 'pgram' function to generate periodograms)

###### Instructions:
* (placeholder for instructions on jupyter notebook and running sql from that once the sql code has been updated from text files to the notebooks)
* Download the latest Tableau workbook (without extract) file that has the .twb extension from the twb/ directory
* Open the file and provide the credentials when it asks for it
* Open the file and click on the Dashboard tab
* Dashboard has several panels whose output will be described below
* NOTE: If any table names or schema names were changed when running the code, the workbook will not pick those up. New plots will need to be generated or the easier solution is the change the table names back to what the workbook expects