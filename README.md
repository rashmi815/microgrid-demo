# microgrid-demo

Microgrid [dataset] (http://smart.cs.umass.edu/download.php?t=microgrid) from the [UMASS Trace repository] (http://traces.cs.umass.edu/index.php/Smart/Smart)


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


## Demo the running of the code and the results by live-connecting to the database
###### Requires:
* Tableau installed on the machine from which you will view the dashboard (e.g. your laptop)
    * Make sure the version of Tableau installed can open the file
* GPDB or Postgres installation (on laptop /VM / in the cloud / on-premise) with the required results tables
    * IP address to device hosting GPDB or Postgres and ability to successfully connect to that IP
    * Login credentials to the GPDB or Postgres database
    * MADlib installed (the demo runs the 'k-means' function)
    * PL/R installed (the demo uses the 'pgram' function to generate periodogram features)

###### Instructions:
* Download the latest Tableau workbook (without extract) file that has the .twb extension from the twb/ directory
* Open the file and click on any sheet
* Open the file and click on the Dashboard tab
* Dashboard has several panels whose output will be described below