# dwd_data_crawler
dwd_data_crawler is a micro service to crawl data from DWD (Deutscher Wetter
  Dienst) and store it for later use in a file system.

## Usage
dwd_data_crawler is configured by means of environment variables. Currently the
following environment variables are supported:
 * `DOWNLOAD_DIRECTORY_BASE_PATH`: Base path of the directory where the downloaded
 files shall be stored. This is a mandatory parameter.
 * `COSMO_DE_CRAWL_RETRY_WAIT_MINUTES`: Number of **minutes** to wait before next attempt to
 crawl for COSMO DE data, when crawling data failed. This is an optional parameter.
 Standard value is `1`.
 * `COSMO_DE_COMPLETE_CYCLE_WAIT_MINUTES`: Number of **minutes** to wait before start next
 crawl cycle for COSMO DE data, once the current cycle is finished. This is an optional parameter.
 Standard values is `10`.
 * `FORECAST_CRAWL_RETRY_WAIT_MINUTES`: Number of **minutes** to wait before next attempt to
 crawl for forecast data, when crawling data failed. This is an optional parameter.
 Standard value is `1`.
 * `FORECAST_COMPLETE_CYCLE_WAIT_MINUTES`: Number of **minutes** to wait before start next
 crawl cycle for forecast data, once the current cycle is finished. This is an optional parameter.
 Standard values is `120`.
 * `REPORT_CRAWL_RETRY_WAIT_MINUTES`: Number of **minutes** to wait before next attempt to
 crawl for report data, when crawling data failed. This is an optional parameter.
 Standard value is `1`.
 * `REPORT_COMPLETE_CYCLE_WAIT_MINUTES`: Number of **minutes** to wait before start next
 crawl cycle for report data, once the current cycle is finished. This is an optional parameter.
 Standard values is `30`.

Sample call:
```
$ DOWNLOAD_DIRECTORY_BASE_PATH=/mnt/download_volume node index.js
```

## Basic idea
The basic idea behind dwd_data_crawler is to query and store data from
opendata.dwd.de. This allows for building a local data storage of historical and
forecast data for later use in research and development. opendata.dwd.de is a
free service offered by DWD which provides weather and climate data based on
real measurements and forecast calculations.

As data on opendata.dwd.de is overwritten on a regular basis, dwd_data_crawler
fetches data cyclically. As opendata.dwd.de stores data for different points in
time using the same file path, dwd_data_crawler uses a slightly different folder
structure than opendata.dwd.de.

### /weather/local_forecasts/poi
#### Filepath on opendata.dwd.de
`/weather/local_forecasts/poi/$STATION_ID-MOSMIX.csv`

where
* `$STATION_ID` is the station id the forecast data refers to.

#### Filepath on local storage
`$DOWNLOAD_DIRECTORY_BASE_PATH/weather/local_forecasts/poi/$YYYY$MM$DD$HH/$STATION_ID-MOSMIX.csv`

where
* `$DOWN_LOAD_DIRECTORY_BASE_PATH` is the base path for the downloaded data on the local storage
* `$YYYY` is the 4 digit year the first forecast data refers to
* `$MM` is the 2 digit month the first forecast data refers to (01 to 12)
* `$DD` is the 2 digit day of month the first forecast data refers to (01 to 31)
* `$HH` is the 2 digit hour of day the first foreact data refers to (06 or 18)
* `$STATION_ID` is the station id the forecast data refers to.

If you want to load the forecast for 9th February 2018 starting at 6 am (UTC) of station 01271 the corresponding data can be found in the file `/$DOWNLOAD_DIRECTORY_BASE_PATH/weather/local_forecasts/poi/2018020906/01271-MOSMIX.csv`

### /weather/weather_reports/poi
#### Filepath on opendata.dwd.de
`/weather/weather_reports/poi/$STATION_ID-BEOB.csv`

where
* `$STATION_ID` is the station id the report data refers to.

#### Filepath on local storage
`$DOWNLOAD_DIRECTORY_BASE_PATH/weather/weather_reports/poi/YYYYMMDD/STATION_ID-MOSMIX.csv`

where
* `$DOWN_LOAD_DIRECTORY_BASE_PATH` is the base path for the downloaded data on the local storage
* `$YYYY` is the 4 digit year the first report data refers to
* `$MM` is the 2 digit month the first report data refers to (01 to 12)
* `$DD` is the 2 digit day of month the first report data refers to (01 to 31)
* `$STATION_ID` is the station id the report data refers to.

If you want to load the reported data for 9th February 2018 (UTC) of station 01271 the corresponding data can be found in the file `/$DOWNLOAD_DIRECTORY_BASE_PATH/weather/local_forecasts/poi/20180209/01271-BEOB.csv`

### /weather/cosmo/de/grib
#### Filepath on opendata.dwd.de
`/weather/cosmo/de/grib/$HH/$dwd_voi/COSMODE_single_level_elements_$DWD_VOI_$YYYY$MM$DD$HH_$FH.grib2.bz2`

where
* `$HH` is the fore cast run hour the data refers to (00, 03, 06, 09, 12, 15, 18, 21)
* `$dwd_voi` is the encoding of the value of interest in lowercase letters (e.g. alb_rad)
* `$DWD_VOI` is the encoding of the value of interest in uppercase letters (e.g. ALB_RAD)
* `$YYYY` is the 4 digit year of the forecast run the data refers to
* `$MM` is the 2 digit month of the forecast run the data refers to (01 to 12)
* `$DD` is the 2 digit day of month of the forecast run the data refers to (01 to 31)
* `$FH` is the 3 digit forecast hour of the forecast run the data refers to (e.g. 021 for forecast in 21 hours as of $YYYY-$MM-$DD $HH:00:00)

#### Filepath on local storage
`/$DOWNLOAD_DIRECTORY_BASE_PATH/cosmo/de/grib/$YYYY$MM$DD$HH/$dwd_voi/COSMODE_single_level_elements_$DWD_VOI_$YYYY$MM$DD$HH_$FH.grib2.bz2`

where
* `$DOWN_LOAD_DIRECTORY_BASE_PATH` is the base path for the downloaded data on the local storage
* `$YYYY` is the 4 digit year of the forecast run the data refers to
* `$MM` is the 2 digit month of the forecast run the data refers to (01 to 12)
* `$DD` is the 2 digit day of month of the forecast run the data refers to (01 to 31)
* `$HH` is the fore cast run hour the data refers to (00, 03, 06, 09, 12, 15, 18, 21)
* `$dwd_voi` is the encoding of the value of interest in lowercase letters (e.g. alb_rad)
* `$DWD_VOI` is the encoding of the value of interest in uppercase letters (e.g. ALB_RAD)
* `$FH` is the 3 digit forecast hour of the forecast run the data refers to (e.g. 021 for forecast in 21 hours as of $YYYY-$MM-$DD $HH:00:00)

## How it works
The dwd_data_crawler is implemented as three endless loops that run concurrently in a single node.js proces.
![main](./docs/main.svg "main routine of index.js")

* reportMain cyclically queries the data from /weather/weather_reports/poi
* forecastMain cyclically queries the data from /weather/local_forecasts/poi
* COSMO_DEMain cyclically queries the data from /weather/cosmo/de/grib

### reportMain
Reports are queried in an endless loop as shown in the followig state chart.
![reportMain](./docs/report_loop.svg "Endless loop for querying reports")

At the beginning of each loop the IP address of opendata.dwd.de is queried, as due to too many requests the DNS refuses services, when all requests are made by domain name.

When the IP address is known all available paths of report files are queried as a list of items. Afterwards, for each item in the list a download is performed. The download is implemented in a way, that three attempts are made to download the (this due to potential rate limiting being active at DWD). Once all items have been downloaded successully, a pause is initiated with a parameterizable wait time of `REPORT_COMPLETE_CYCLE_WAIT_MINUTES`.

### forecastMain
![forecastMain](./docs/forecast_loop.svg "Endless loop for querying forecasts")

### COSMO_DEMain
![COSMO_DEMain](./docs/cosmo_de_loop.svg "Endless loop for querying cosmo de forecasts")
