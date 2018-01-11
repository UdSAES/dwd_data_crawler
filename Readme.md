# dwd_data_crawler
dwd_data_crawler is a micro service to crawl data from DWD (Deutscher Wetter
  Dienst) and store it for later use in a file system.

## Usage
dwd_data_crawler is configured by means of environment variables. Currently the
following environment variables are supported:
 * `DOWNLOAD_DIRECTORY_BASE_PATH`: Base path of the directory where the downloaded
 files shall be stored. This is a mandatory parameter.
 * `CRAWL_RETRY_WAIT_MINUTES`: Number of minutes to wait before next attempt to
 crawl for data, when crawling data failed. This is an optional parameter.
 Standard value is `1`.
 * `COMPLETE_CYCLE_WAIT_MINUTES`: Number of minutes to wait before start next
 crawl cycle, once the current cycle is finished. This is an optional parameter.
 Standard values is `10`.

Sample call:
```
$ DOWNLOAD_DIRECTORY_BASE_PATH=/mnt/download_volume node index.js
```
