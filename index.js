'use strict'

const EXIT_CODES = {
  DOWNLOAD_DIRECTORY_BASE_PATH_NIL_ERROR: 1,
  STORE_DOWNLOAD_FILE_ERROR: 2
}

var bunyan = require('bunyan')
var log = bunyan.createLogger({name: 'dwd_data_crawler'})

const {promisify} = require('util')
const _ = require('lodash')
const dwd_grib = require('./lib/dwd_grib')
const delay = require('delay')
const fs = require('fs-extra')
const processenv = require('processenv')
const request = require('request-promise-native')
const path = require('path')
const lookup = promisify(require('dns').lookup)
const {URL} = require('url')
const execFile = promisify(require('child_process').execFile)


const DWD_COSMO_DE_BASE_URL = 'https://opendata.dwd.de/weather/cosmo/de/grib/'
const DWD_FORECAST_BASE_URL = 'https://opendata.dwd.de/weather/local_forecasts/poi/'
const DWD_REPORT_BASE_URL = 'https://opendata.dwd.de/weather/weather_reports/poi/'

const DOWNLOAD_DIRECTORY_BASE_PATH = processenv('DOWNLOAD_DIRECTORY_BASE_PATH')
const COSMO_DE_CRAWL_RETRY_WAIT_MINUTES = processenv('COSMO_DE_CRAWL_RETRY_WAIT_MINUTES') || 1
const COSMO_DE_COMPLETE_CYCLE_WAIT_MINUTES = processenv('COSMO_DE_COMPLETE_CYCLE_WAIT_MINUTES') || 10

const FORECAST_CRAWL_RETRY_WAIT_MINUTES = processenv('CRAWL_RETRY_WAIT_MINUTES') || 1
const FORECAST_COMPLETE_CYCLE_WAIT_MINUTES = processenv('COMPLETE_CYCLE_WAIT_MINUTES') || 120

const REPORT_CRAWL_RETRY_WAIT_MINUTES = processenv('CRAWL_RETRY_WAIT_MINUTES') || 1
const REPORT_COMPLETE_CYCLE_WAIT_MINUTES = processenv('COMPLETE_CYCLE_WAIT_MINUTES') || 30


// check if necessery DOWNLOAD_DIRECTORY_BASE_PATH env var is given
if (_.isNil(DOWNLOAD_DIRECTORY_BASE_PATH)) {
  log.fatal('no download directory base path (env variable DOWNLOAD_DIRECTORY_BASE_PATH) given')
  process.exit(EXIT_CODES.DOWNLOAD_DIRECTORY_BASE_PATH_NIL_ERROR)
}

log.info('download directory base path is: ' +  DOWNLOAD_DIRECTORY_BASE_PATH)

function getDataForLocationInGrib(grib, lo, la) {
  const header = grib.header
  const data = grib.data

  const numberOfColumns = 1 + Math.round((header.lo2 - header.lo1) / header.dx)

  const column = Math.round((lo - header.lo1) / header.dx)
  const row = Math.round((la - header.la1) / header.dy)

  return data[(numberOfColumns * row) + column]
}


/**
 * convertDomainUrlToIPUrl asynchronously queries the IPv4 address for a given
 * host using the lookup method of the node.js dns package
 * @param  {String} domainUrlString the url to query the IP address for
 * @return {String}                 the ip address for the url
 */
async function convertDomainUrlToIPUrl(domainUrlString) {
  const domainUrl = new URL(domainUrlString)

  try {
    var ip = await lookup(domainUrl.hostname)
    ip = ip.address
  } catch (error) {
    log.error('lookup error')
    log.error(error)
    throw error
  }
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
async function downloadFile(url) {
  var attempts = 0
  for(;;) {
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
async function reportMain() {
  for(;;) {
    // Using the IP address instead of domain is necessary as with each https
    // request for data based on the url a DNS resolve is performed. After
    // several thousand requests within a short time the DNS server rejects
    // resvolving domain names to IP addresses
    // --> work around: query IP once per cyclce and perform http requests based
    // on the IP instead of the domain name
    const ipBaseUrl = await convertDomainUrlToIPUrl(DWD_REPORT_BASE_URL)

    var listOfFiles = null

    // step 1: crawl list of available grib files
    for (;;) {
      log.info('crawling list of available files at ' + ipBaseUrl + ' ...')

      try {
        listOfFiles = await dwd_grib.crawlListOfFilePaths(ipBaseUrl)
        break
      } catch (error) {
        log.error(error, 'crawling list of report files failed')
      }

      log.info('waiting ' + REPORT_CRAWL_RETRY_WAIT_MINUTES + ' minutes before starting next retry for reports')
      await delay(REPORT_CRAWL_RETRY_WAIT_MINUTES * 60 * 1000)
    }

    log.info('crawl for reports revealed ' + listOfFiles.length + ' files')

    // step 2: download
    for (var i = 0; i < listOfFiles.length; i++) {

      // wait before processing next file
      await delay(1)

      const url = listOfFiles[i]

      try {
        var binaryContent = await downloadFile(url)
        var textContent = binaryContent.toString('utf8')
        var table = dwd_grib.convertCsv2Json(textContent, ';')
        var dateString = table[3][0]
        var timeString = table[3][1]
      } catch (error) {
        log.error({error: error, url: url}, 'an error occured while downloading and parse the csv file')
        continue
      }

      if (timeString !== '00:00') {
        continue

      }
      dateString = table[4][0]

      const urlTokens = url.split('/')
      const fileName = urlTokens[urlTokens.length - 1]
      const directoryPath = path.join(DOWNLOAD_DIRECTORY_BASE_PATH, 'reports', dateString)
      const targetFilePath = path.join(directoryPath, fileName)
      const exists = await fs.pathExists(targetFilePath)

      if (exists) {
        continue
      }

      try {
        await fs.ensureDir(directoryPath)
        await fs.writeFile(targetFilePath, binaryContent, {encoding: null})
      } catch (error) {
        log.fatal({error: error, filePath: targetFilePath}, 'storing file at ' + targetFilePath + ' failed')
        process.exit(1)
      }
    }

    // wait COMPLETE_CYCLE_WAIT_MINUTES minutes before polling for new files
    log.info('waiting ' + REPORT_COMPLETE_CYCLE_WAIT_MINUTES + ' minutes before starting next reports cycle')
    await delay(REPORT_COMPLETE_CYCLE_WAIT_MINUTES * 60 * 1000)
  }
}

/**
 * reportMain asynchronously downloads the forecast data in an endless lookup
 */
async function forecastMain() {
  for(;;) {
    // Using the IP address instead of domain is necessary as with each https
    // request for data based on the url a DNS resolve is performed. After
    // several thousand requests within a short time the DNS server rejects
    // resvolving domain names to IP addresses
    // --> work around: query IP once per cyclce and perform http requests based
    // on the IP instead of the domain name
    const ipBaseUrl = await convertDomainUrlToIPUrl(DWD_FORECAST_BASE_URL)

    var listOfFiles = null

    // step 1: crawl list of available grib files
    for (;;) {
      log.info('crawling list of available files at ' + ipBaseUrl + ' ...')

      try {
        listOfFiles = await dwd_grib.crawlListOfFilePaths(ipBaseUrl)
        break
      } catch (error) {
        log.error(error, 'crawling list of files failed')
      }

      log.info('waiting ' + FORECAST_CRAWL_RETRY_WAIT_MINUTES + ' minutes before starting next retry for forecast')
      await delay(FORECAST_CRAWL_RETRY_WAIT_MINUTES * 60 * 1000)
    }

    log.info('crawl for forecast revealed ' + listOfFiles.length + ' files')
    // step 2: download
    for (var i = 0; i < listOfFiles.length; i++) {
      // wait before processing next file
      await delay(1)

      const url = listOfFiles[i]

      if (i % 100 === 0) {
        log.info('forecast is handling file ' + url)
      }

      try {
        var binaryContent = await downloadFile(url)
        var textContent = binaryContent.toString('utf8')
        textContent = textContent.replace(/\r\n/g, '\n')
        const lines = textContent.split('\n')
        var dateString = lines[3].split(';')[0]
      } catch (error) {
        log.error({error: error, url: url}, 'an error occured while downloading and parse the csv file')
        continue
      }

      const urlTokens = url.split('/')
      const fileName = urlTokens[urlTokens.length - 1]
      const directoryPath = path.join(DOWNLOAD_DIRECTORY_BASE_PATH, 'forecasts', dateString)
      const targetFilePath = path.join(directoryPath, fileName)

      const exists = await fs.pathExists(targetFilePath)

      if (exists) {
        continue
      }

      try {
        await fs.ensureDir(directoryPath)
        await fs.writeFile(targetFilePath, binaryContent, {encoding: null})
      } catch (error) {
        log.fatal({error: error, filePath: targetFilePath}, 'storing file at ' + targetFilePath + ' failed')
        process.exit(1)
      }
    }

    // wait COMPLETE_CYCLE_WAIT_MINUTES minutes before polling for new files
    log.info('waiting ' + FORECAST_COMPLETE_CYCLE_WAIT_MINUTES + ' minutes before starting next forecast cycle')
    await delay(FORECAST_COMPLETE_CYCLE_WAIT_MINUTES * 60 * 1000)
  }
}


/**
 * COSMO_DEMain asynchronously downloads the COSMO DE data in an endless lookup
 */
async function COSMO_DEMain() {
  for(;;) {

    // Using the IP address instead of domain is necessary as with each https
    // request for data based on the url a DNS resolve is performed. After
    // several thousand requests within a short time the DNS server rejects
    // resvolving domain names to IP addresses
    // --> work around: query IP once per cyclce and perform http requests based
    // on the IP instead of the domain name
    const ipBaseUrl = await convertDomainUrlToIPUrl(DWD_COSMO_DE_BASE_URL)

    var listOfFiles = null

    // step 1: crawl list of available grib2 files
    for (;;) {
      log.info('crawling list of available files at ' + ipBaseUrl + ' ...')

      try {
        listOfFiles = await dwd_grib.crawlListOfGrib2FilePaths(ipBaseUrl)
        break
      } catch (error) {
        log.error(error, 'crawling list of grib2 files failed')
      }

      log.info('waiting ' + COSMO_DE_CRAWL_RETRY_WAIT_MINUTES + ' before starting next retry for grib')
      await delay(COSMO_DE_CRAWL_RETRY_WAIT_MINUTES * 60 * 1000)
    }

    log.info('crawl for grib revealed ' + listOfFiles.length + ' files')

    // step 2: download and store all files, if they have not been downloaded, yet
    for (var i = 0; i < listOfFiles.length; i++) {
      // wait before processing next file
      await delay(1)

      const url = listOfFiles[i]
      const urlTokens = url.split('/')
      const sourceQuantity = urlTokens[urlTokens.length - 2]
      const fileNameTokens = urlTokens[urlTokens.length - 1].split('_')
      const dateTimeString = fileNameTokens[fileNameTokens.length - 2]

      const directoryPath = path.join(DOWNLOAD_DIRECTORY_BASE_PATH, 'grib', dateTimeString, sourceQuantity)
      const filePath =  path.join(
        directoryPath,
        urlTokens[urlTokens.length - 1].replace('bz2', 'lz4')
      )

      const exists = await fs.pathExists(filePath)

      if (exists) {
        continue
      }

      try {
        log.info('downloading ' + url)
        var content = await downloadFile(url)
      } catch (error) {
        log.error(error, 'downloading ' + url + ' failed')
        continue
      }

      try {
        await fs.ensureDir(directoryPath)
        await fs.writeFile(filePath.replace('lz4', 'bz2'), content, {encoding: null})
        await execFile('bzip2', ['-d', filePath.replace('lz4', 'bz2')])
        await execFile('lz4', ['-z9', filePath.replace('\.lz4', ''), filePath])
        await fs.unlink(filePath.replace('\.lz4', ''))
      } catch (error) {
        log.error(error, 'decompressing bzip2 file ' + url + ' failed')
        continue
      }

      /*
      try {
        await fs.writeFile(targetFilePath, content, {encoding: null})
      } catch (error) {
        log.fatal({error: error, filePath: filePath}, 'storing file at ' + filePath + ' failed')
        process.exit(1)
      }
      */
    }

    // wait COMPLETE_CYCLE_WAIT_MINUTES minutes before polling for new files
    log.info('waiting ' + COSMO_DE_COMPLETE_CYCLE_WAIT_MINUTES + ' minutes before starting next COSMO DE cycle')
    await delay(COSMO_DE_COMPLETE_CYCLE_WAIT_MINUTES * 60 * 1000)
  }
}


// start the three concurrent loops to query forecast, report and COSMO DE data
forecastMain()
reportMain()
COSMO_DEMain()
