'use strict'

/*const {JSDOM} = require('jsdom')
const $ = require('jquery')(new JSDOM().window)*/

const cheerio = require('cheerio')
const {promisify} = require('util')
const {VError} = require('verror')
const request = require('request-promise-native')
const uuid4 = require('uuid/v4')
//const tmp = require('tmp-promise')
const path = require('path')
const _ = require('lodash')
const fs = require('fs-extra')
const assert = require('assert')
const {Url} = require('url')

//const Bunzip = require('seek-bzip')
//const grib2json = require('weacast-grib2json')

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function convertCsv2Json(csv, delimiter) {
  assert(_.isString(csv))
  assert(_.isString(delimiter))
  csv = csv.replace(/\r\n/g, '\n')
  const lines = csv.split('\n')

  const result = []
  _.forEach(lines, (line) => {
    const columns = line.split(delimiter)
    result.push(columns)
  })

  return result
}


async function crawlListOfFilePaths(baseUrl) {
  const listOfFiles = []

  const result = await request({
    method: 'get',
    url: baseUrl,
    simple: true,
    strictSSL: false
  })

  const $ = cheerio.load(result)
  const as = $('a')

  for (var i = 0; i < as.length; i++) {
    const href = $(as[i]).attr('href')
    if (href === '../') {
      continue
    }

    listOfFiles.push(baseUrl + href)
  }

  return listOfFiles
}


async function crawlListOfGribFilePaths(baseUrl, listOfFiles) {
  if (_.isNil(listOfFiles)) {
    listOfFiles = []
  }

  var result
  var attempt = 0
  for (;;) {
    try {
      result = await request({
        method: 'get',
        url: baseUrl,
        simple: true,
        strictSSL: false
      })
      break
    } catch (error) {
      attempt++
      if (attempt > 3) {
        throw(error)
      }
    }
  }


  const $ = cheerio.load(result)
  const as = $('a')

  for (var i = 0; i < as.length; i++) {
    const href = $(as[i]).attr('href')

    if (href === '../') {
      continue
    }

    if (href.endsWith('.grib2.bz2')) {
      if (!href.includes('_org_') && href.includes('single_level')) {
        listOfFiles.push(baseUrl + href)
      }
      continue
    }

    if (href.includes('COSMODE')) {
      continue
    }

    await crawlListOfGribFilePaths(baseUrl + href, listOfFiles)
  }

  return listOfFiles
}

/*
async function getGribData(url) {

  const content = await request({
    method: 'get',
    url: url,
    simple: true,
    encoding: null
  })

  const decompressed = Bunzip.decode(content)

  const foBinary = await tmp.file()
  await fs.writeFile(foBinary.path, decompressed)

  const foJson = await tmp.file()
  await grib2json(foBinary.path, {
    data: true,
    output: foJson.path
  })

  const gribData = await fs.readJson(foJson.path, {encoding: 'utf8'})

  return gribData
}*/

exports.convertCsv2Json = convertCsv2Json
exports.crawlListOfGribFilePaths = crawlListOfGribFilePaths
exports.crawlListOfFilePaths = crawlListOfFilePaths
