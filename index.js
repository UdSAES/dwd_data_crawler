// dwd_data_crawler
//
// Copyright 2018 The dwd_data_crawler Developers. See the LICENSE file at
// the top-level directory of this distribution and at
// https://github.com/UdSAES/dwd_data_crawler/LICENSE
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// dwd_data_crawler may be freely used and distributed under the MIT license

'use strict'

const EXIT_CODES = {
  DOWNLOAD_DIRECTORY_BASE_PATH_NIL_ERROR: 1,
  STORE_DOWNLOAD_FILE_ERROR: 2
}

const { promisify } = require('util')
const _ = require('lodash')
const dwd_grib = require('./lib/dwd_grib')
const dwd_csv = require('./lib/dwd_csv')
const delay = require('delay')
const fs = require('fs-extra')
const processenv = require('processenv')
const request = require('request-promise-native')
const path = require('path')
const lookup = promisify(require('dns').lookup)
const { URL } = require('url')
const execFile = promisify(require('child_process').execFile)
const moment = require('moment-timezone')
const bunyan = require('bunyan')

const DWD_ICON_D2_BASE_URL = 'https://opendata.dwd.de/weather/nwp/icon-d2/grib/'
const DWD_MOSMIX_BASE_URL =
  'https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_L/single_stations/'
const DWD_REPORT_BASE_URL = 'https://opendata.dwd.de/weather/weather_reports/poi/'

const DOWNLOAD_DIRECTORY_BASE_PATH = processenv('DOWNLOAD_DIRECTORY_BASE_PATH')
const DOWNLOAD_ICON_D2 = Boolean(processenv('DOWNLOAD_ICON_D2')) || false
const DOWNLOAD_MOSMIX = Boolean(processenv('DOWNLOAD_MOSMIX')) || false
const DOWNLOAD_BEOB = Boolean(processenv('DOWNLOAD_BEOB')) || false
const ICON_D2_CRAWL_RETRY_WAIT_MINUTES =
  processenv('ICON_D2_CRAWL_RETRY_WAIT_MINUTES') || 1
const ICON_D2_COMPLETE_CYCLE_WAIT_MINUTES =
  processenv('ICON_D2_COMPLETE_CYCLE_WAIT_MINUTES') || 10
const FORECAST_CRAWL_RETRY_WAIT_MINUTES =
  processenv('FORECAST_CRAWL_RETRY_WAIT_MINUTES') || 1
const FORECAST_COMPLETE_CYCLE_WAIT_MINUTES =
  processenv('FORECAST_COMPLETE_CYCLE_WAIT_MINUTES') || 120
const REPORT_CRAWL_RETRY_WAIT_MINUTES =
  processenv('REPORT_CRAWL_RETRY_WAIT_MINUTES') || 1
const REPORT_COMPLETE_CYCLE_WAIT_MINUTES =
  processenv('REPORT_COMPLETE_CYCLE_WAIT_MINUTES') || 30
const LOG_LEVEL = String(processenv('LOG_LEVEL') || 'info')

// Instantiate logger
const log = bunyan.createLogger({
  name: 'dwd_data_crawler',
  serializers: bunyan.stdSerializers,
  level: LOG_LEVEL
})
log.info('instantiation of service initiated')

// check if necessery DOWNLOAD_DIRECTORY_BASE_PATH env var is given
if (_.isNil(DOWNLOAD_DIRECTORY_BASE_PATH)) {
  log.fatal(
    'no download directory base path given (DOWNLOAD_DIRECTORY_BASE_PATH missing)'
  )
  process.exit(EXIT_CODES.DOWNLOAD_DIRECTORY_BASE_PATH_NIL_ERROR)
} else {
  log.info('DOWNLOAD_DIRECTORY_BASE_PATH is set to ', DOWNLOAD_DIRECTORY_BASE_PATH)
}

/* We need this for later use
 function getDataForLocationInGrib(grib, lo, la) {
   const header = grib.header
  const data = grib.data

  const numberOfColumns = 1 + Math.round((header.lo2 - header.lo1) / header.dx)

  const column = Math.round((lo - header.lo1) / header.dx)
  const row = Math.round((la - header.la1) / header.dy)

  return data[(numberOfColumns * row) + column]
} */

/**
 * convertDomainUrlToIPUrl asynchronously queries the IPv4 address for a given
 * host using the lookup method of the node.js dns package
 * @param  {String} domainUrlString the url to query the IP address for
 * @return {String}                 the ip address for the url
 */
async function convertDomainUrlToIPUrl (domainUrlString) {
  const domainUrl = new URL(domainUrlString)

  let ip = await lookup(domainUrl.hostname)
  ip = ip.address
  domainUrl.hostname = ip
  return domainUrl.toString()
}

/**
 * downloadFile asynchronously downloads the content from the given url
 * - the function includes a retry mechanism in order to handle temporary errors
 * - currently three attempts are made to download before finally failing
 * - between two attempts there is a wait time of 10ms
 * @param  {String} url the url to download the data from
 * @return
 */
async function downloadFile (url) {
  let attempts = 0
  for (;;) {
    try {
      const result = await request({
        method: 'get',
        url: url,
        encoding: null,
        simple: true,
        strictSSL: false
      })

      return result
    } catch (error) {
      attempts++
      log.warn(error)
      await delay(10)
      if (attempts > 3) {
        throw error
      }
    }
  }
}

/**
 * reportMain asynchronously downloads the report data in an endless lookup
 */
async function reportMain () {
  log.info('start crawling measured climate data')
  for (;;) {
    // Using the IP address instead of domain is necessary as with each https
    // request for data based on the url a DNS resolve is performed. After
    // several thousand requests within a short time the DNS server rejects
    // resvolving domain names to IP addresses
    // --> work around: query IP once per cyclce and perform http requests based
    // on the IP instead of the domain name
    try {
      var ipBaseUrl = await convertDomainUrlToIPUrl(DWD_REPORT_BASE_URL)
    } catch (error) {
      log.error(error, 'resolving IP-address for DWD_REPORT_BASE_URL failed')
      await delay(REPORT_CRAWL_RETRY_WAIT_MINUTES * 60 * 1000)
      continue
    }

    let listOfFiles = null
    let numberOfFilesDownloaded = 0

    // step 1: crawl list of available grib files
    for (;;) {
      log.info('crawling list of available files at ' + ipBaseUrl + ' ...')

      try {
        listOfFiles = await dwd_grib.crawlListOfFilePaths(ipBaseUrl)
        break
      } catch (error) {
        log.error(error, 'crawling list of report files failed')
      }

      log.info(
        'waiting ' +
          REPORT_CRAWL_RETRY_WAIT_MINUTES +
          ' minutes before starting next retry for reports'
      )
      await delay(REPORT_CRAWL_RETRY_WAIT_MINUTES * 60 * 1000)
    }

    log.info('crawling for reports revealed ' + listOfFiles.length + ' files')

    // step 2: download
    for (let i = 0; i < listOfFiles.length; i++) {
      // wait before processing next file
      await delay(1)

      const url = listOfFiles[i]
      try {
        const binaryContent = await downloadFile(url)
        var textContent = binaryContent.toString('utf8')
        var table = dwd_csv.parseCSV(textContent)
      } catch (error) {
        log.error(
          { error: error, url: url },
          'an error occured while downloading and parsing ' + url
        )
        continue
      }
      numberOfFilesDownloaded = numberOfFilesDownloaded + 1

      // iterate all content lines and extract dates
      var dates = {}
      _.forEach(table.slice(3), (row) => {
        try {
          var m = moment.tz(row[0], 'DD.MM.YYYY', 'UTC')
        } catch (error) {
          log.error(
            { error: error, url: url },
            'an error occured while handling the csv file'
          )
          return
        }

        if (!m.isValid()) {
          return
        }

        const dateString = m.format('YYYYMMDD')
        dates[dateString] = dateString
      })

      dates = _.keys(dates)

      for (let j = 0; j < dates.length; j++) {
        const dateString = dates[j]
        const urlTokens = url.split('/')
        const fileName = urlTokens[urlTokens.length - 1]
        const targetDirectory = path.join(
          DOWNLOAD_DIRECTORY_BASE_PATH,
          'weather',
          'weather_reports',
          'poi',
          dateString
        )
        const targetFilePath = path.join(targetDirectory, fileName)

        // check if target file already exists
        await fs.ensureDir(targetDirectory)
        const exists = await fs.pathExists(targetFilePath)
        if (exists) {
          try {
            const currentContent = await fs.readFile(targetFilePath, {
              encoding: 'utf8'
            })
            const newContent = dwd_csv.mergeCSVContents(
              currentContent,
              textContent,
              moment.tz(dateString, 'YYYYMMDD', 'UTC').format('DD.MM.YY')
            )
            await fs.writeFile(targetFilePath, newContent, { encoding: 'utf8' })
          } catch (error) {
            log.error(
              { error: error.toString(), url: url },
              'an error occured while reading, merging, and writing the existing file'
            )
          }
        } else {
          try {
            const newTable = table.slice(0, 3)
            _.forEach(table.slice(3), (row) => {
              if (
                row[0] !== moment.tz(dateString, 'YYYYMMDD', 'UTC').format('DD.MM.YY')
              ) {
                return
              }

              newTable.push(row)
            })
            await fs.writeFile(targetFilePath, dwd_csv.generateCSV(newTable), {
              encoding: 'utf8'
            })
          } catch (error) {
            log.error(
              { error: error, url: url },
              'an error occured while writing the new file'
            )
          }
        }
      }
    }
    log.info('downloaded ' + numberOfFilesDownloaded + ' new REPORT files')

    // wait COMPLETE_CYCLE_WAIT_MINUTES minutes before polling for new files
    log.info(
      'waiting ' +
        REPORT_COMPLETE_CYCLE_WAIT_MINUTES +
        ' minutes before starting next reports cycle'
    )
    await delay(REPORT_COMPLETE_CYCLE_WAIT_MINUTES * 60 * 1000)
  }
}

/**
 * crawlMOSMIXasKMZ asynchronously downloads the MOSMIX_L-forecast data in an endless lookup
 */
async function crawlMOSMIXasKMZ () {
  log.info('start crawling MOSMIX-forecasts provided as .kmz-files')
  for (;;) {
    // Using the IP address instead of domain is necessary as with each https
    // request for data based on the url a DNS resolve is performed. After
    // several thousand requests within a short time the DNS server rejects
    // resvolving domain names to IP addresses
    // --> work around: query IP once per cyclce and perform http requests based
    // on the IP instead of the domain name
    try {
      var ipBaseUrl = await convertDomainUrlToIPUrl(DWD_MOSMIX_BASE_URL)
    } catch (error) {
      log.error(error, 'resolving IP-address for DWD_MOSMIX_BASE_URL failed')
      await delay(FORECAST_CRAWL_RETRY_WAIT_MINUTES * 60 * 1000)
      continue
    }

    let listOfStations = null
    let listOfFiles = []
    let numberOfFilesDownloaded = 0

    // Crawl list of available stations
    for (;;) {
      log.info('crawling list of available stations at ' + ipBaseUrl + '...')

      try {
        listOfStations = await dwd_grib.crawlListOfFilePaths(ipBaseUrl)
        break
      } catch (error) {
        log.error(error, 'crawling list of stations failed')
      }

      log.info(
        'waiting ' +
          FORECAST_CRAWL_RETRY_WAIT_MINUTES +
          ' minutes before starting next retry for MOSMIX_L'
      )
      await delay(FORECAST_CRAWL_RETRY_WAIT_MINUTES * 60 * 1000)
    }

    log.info(
      'crawling for MOSMIX_L-forecasts revealed ' + listOfStations.length + ' stations'
    )

    // Build list of available .kmz-files
    for (var i = 0; i < listOfStations.length; i++) {
      const url = listOfStations[i] + 'kml/'
      const urlElements = _.split(listOfStations[i], '/')
      var stationID = urlElements[urlElements.length - 2]
      // log.info('crawling available files for station ' + stationID)

      try {
        const files = await dwd_grib.crawlListOfFilePaths(url)
        listOfFiles = _.concat(listOfFiles, files)
      } catch (error) {
        log.error(error, 'crawling list of files for station ' + stationID + ' failed')
      }
    }

    log.info(
      'crawling for MOSMIX_L-forecasts revealed ' + listOfFiles.length + ' files'
    )

    // Download all files unless they already exist
    for (var i = 0; i < listOfFiles.length; i++) {
      await delay(1) // wait before processing next file

      const url = listOfFiles[i]
      const fileName = _.last(_.split(url, '/'))
      const timeStamp = _.split(fileName, '_')[2]
      var stationID = _.split(_.split(fileName, '_')[3], '.')[0]
      const extension = _.split(fileName, '.')[1]
      const fileNameOnDisk = stationID + '-MOSMIX.' + extension

      const directoryPath = path.join(
        DOWNLOAD_DIRECTORY_BASE_PATH,
        'weather',
        'local_forecasts',
        'mos',
        timeStamp
      )
      const targetFilePath = path.join(directoryPath, fileNameOnDisk)

      const exists = await fs.pathExists(targetFilePath)

      // Skip this file if it already exists, otherwise download and save it
      if (exists) {
        continue
      }

      try {
        var binaryContent = await downloadFile(url)
        log.debug('downloading new forecast ' + fileName)
      } catch (error) {
        log.error(
          { error: error, url: url },
          'an error occured while downloading ' + fileName
        )
        continue
      }

      try {
        await fs.ensureDir(directoryPath)
        await fs.writeFile(targetFilePath, binaryContent, { encoding: null })
      } catch (error) {
        log.fatal(
          { error: error, filePath: targetFilePath },
          'storing file at ' + targetFilePath + ' failed'
        )
        process.exit(1)
      }
      numberOfFilesDownloaded = numberOfFilesDownloaded + 1
    }
    log.info('downloaded ' + numberOfFilesDownloaded + ' new MOSMIX-forecasts')

    // Wait COMPLETE_CYCLE_WAIT_MINUTES minutes before polling for new files
    log.info(
      'waiting ' +
        FORECAST_COMPLETE_CYCLE_WAIT_MINUTES +
        ' minutes before starting next forecast cycle'
    )
    await delay(FORECAST_COMPLETE_CYCLE_WAIT_MINUTES * 60 * 1000)
  }
}

/**
 * ICON_D2Main asynchronously downloads the ICON D2 data in an endless lookup
 */
async function ICON_D2Main () {
  log.info('start crawling ICON-D2-forecasts')
  for (;;) {
    // Using the IP address instead of domain is necessary as with each https
    // request for data based on the url a DNS resolve is performed. After
    // several thousand requests within a short time the DNS server rejects
    // resvolving domain names to IP addresses
    // --> work around: query IP once per cyclce and perform http requests based
    // on the IP instead of the domain name

    try {
      var ipBaseUrl = await convertDomainUrlToIPUrl(DWD_ICON_D2_BASE_URL)
    } catch (error) {
      log.error(error, 'resolving IP-address for DWD_ICON_D2_BASE_URL failed')
      await delay(ICON_D2_CRAWL_RETRY_WAIT_MINUTES * 60 * 1000)
      continue
    }

    let listOfFiles = null
    let numberOfFilesDownloaded = 0

    // step 1: crawl list of available grib2 files
    for (;;) {
      log.info('crawling list of available files at ' + ipBaseUrl + ' ...')

      try {
        listOfFiles = await dwd_grib.crawlListOfGrib2FilePaths(ipBaseUrl)
        break
      } catch (error) {
        log.error(error, 'crawling list of grib2 files failed')
      }

      log.info(
        'waiting ' +
          ICON_D2_CRAWL_RETRY_WAIT_MINUTES +
          ' before starting next retry for grib'
      )
      await delay(ICON_D2_CRAWL_RETRY_WAIT_MINUTES * 60 * 1000)
    }

    log.info('crawling for grib revealed ' + listOfFiles.length + ' files')

    // step 2: download and store all files, if they have not been downloaded, yet
    for (let i = 0; i < listOfFiles.length; i++) {
      // wait before processing next file
      await delay(1)

      const url = listOfFiles[i]
      const urlTokens = _.split(url, '/')
      const sourceQuantity = _.nth(urlTokens, -2)
      const fileNameDotSeparated = _.split(_.nth(urlTokens, -1), '.')
      const fileName = _.join(_.dropRight(fileNameDotSeparated, 2), '.')
      const fileExtension = _.join(
        _.drop(fileNameDotSeparated, fileNameDotSeparated.length - 2),
        '.'
      )
      const fileNameTokens = _.split(fileName, '_')

      const model = _.nth(fileNameTokens, 0) // icon-d2
      const scope = _.nth(fileNameTokens, 1) // germany
      const gridType = _.nth(fileNameTokens, 2) // regular-lat-lon
      const levelType = _.nth(fileNameTokens, 3) // single-level
      const run = _.nth(fileNameTokens, 4) // 2021022100
      const step = _.nth(fileNameTokens, 5) // 008
      const level = _.nth(fileNameTokens, 6) // 2d
      const field = _.join(_.drop(fileNameTokens, 7), '_') // aswdifd_s

      if (field !== sourceQuantity) {
        log.error(
          `the identified sourceQuantity ${sourceQuantity} does not match the field ${field} parsed from the filename!`
        )
      }

      const directoryPath = path.join(
        DOWNLOAD_DIRECTORY_BASE_PATH,
        'weather',
        'icon-d2',
        'grib',
        run,
        sourceQuantity
      )
      const filePath = path.join(
        directoryPath,
        _.join([fileName, _.replace(fileExtension, 'bz2', 'lz4')], '.')
      )

      const exists = await fs.pathExists(filePath)

      if (exists) {
        continue
      }

      try {
        log.debug('downloading and storing file ' + url)
        const content = await downloadFile(url)
        await fs.ensureDir(directoryPath)
        await fs.writeFile(filePath.replace('lz4', 'bz2'), content, { encoding: null })
        await execFile('bzip2', ['-d', filePath.replace('lz4', 'bz2')])
        await execFile('lz4', ['-z9', filePath.replace('.lz4', ''), filePath])
        await fs.unlink(filePath.replace('.lz4', ''))
      } catch (error) {
        log.error(error, 'downloading and storing file ' + url + ' failed')
        continue
      }
      numberOfFilesDownloaded = numberOfFilesDownloaded + 1
    }
    log.info('downloaded ' + numberOfFilesDownloaded + ' new ICON-D2-forecasts')

    // wait COMPLETE_CYCLE_WAIT_MINUTES minutes before polling for new files
    log.info(
      'waiting ' +
        ICON_D2_COMPLETE_CYCLE_WAIT_MINUTES +
        ' minutes before starting next ICON-D2 cycle'
    )
    await delay(ICON_D2_COMPLETE_CYCLE_WAIT_MINUTES * 60 * 1000)
  }
}

// Start three concurrent loops to query MOSMIX, ICON-D2 and measurement data

log.debug('DOWNLOAD_ICON_D2 is ', DOWNLOAD_ICON_D2)
log.debug('DOWNLOAD_MOSMIX is ', DOWNLOAD_MOSMIX)
log.debug('DOWNLOAD_BEOB is ', DOWNLOAD_BEOB)

if ((DOWNLOAD_ICON_D2 || DOWNLOAD_MOSMIX || DOWNLOAD_BEOB) === false) {
  log.warn(
    'Nothing to do. Did you set DOWNLOAD_ICON_D2/DOWNLOAD_MOSMIX/DOWNLOAD_BEOB correctly?'
  )
}

if (DOWNLOAD_ICON_D2 === true) {
  ICON_D2Main()
}

if (DOWNLOAD_MOSMIX === true) {
  crawlMOSMIXasKMZ()
}

if (DOWNLOAD_BEOB === true) {
  reportMain()
}
