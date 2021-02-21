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

/* const {JSDOM} = require('jsdom')
const $ = require('jquery')(new JSDOM().window) */

const cheerio = require('cheerio')
const request = require('request-promise-native')
const _ = require('lodash')

/**
 * crawlListOfFilePaths asynchronously queries the list of files in a path
 *
 * files in this context means href attributes of a tags
 * @param  {String} baseUrl the base url where to search for files
 * @return {Array}          the list of files (i.e. complete urls)
 */
async function crawlListOfFilePaths (baseUrl) {
  const listOfFiles = []

  const result = await request({
    method: 'get',
    url: baseUrl,
    simple: true,
    strictSSL: false
  })

  const $ = cheerio.load(result)
  const as = $('a')

  for (let i = 0; i < as.length; i++) {
    const href = $(as[i]).attr('href')
    if (href === '../' || _.includes(href, 'LATEST')) {
      continue
    }

    listOfFiles.push(baseUrl + href)
  }

  return listOfFiles
}

/**
 * crawlListOfGrib2FilePaths asynchronously and recursivele queries a list of
 * that hold certain properties from a given base url
 *
 * files in this context means href attributes of a tags
 * @param  {String} baseUrl     the base url to start the recursive query from
 * @param  {Array} listOfFiles  the list of files where the results shall be stored
 * @return {Array}              the list of files
 */
async function crawlListOfGrib2FilePaths (baseUrl, listOfFiles) {
  if (_.isNil(listOfFiles)) {
    listOfFiles = []
  }

  let result
  let attempt = 0
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
        throw error
      }
    }
  }

  const $ = cheerio.load(result)
  const as = $('a')

  for (let i = 0; i < as.length; i++) {
    const href = $(as[i]).attr('href')

    if (!_.isString(href)) {
      continue
    }

    if (href === '../') {
      continue
    }

    // currently only single-level files in regular coordinates are of interest
    if (href.endsWith('.grib2.bz2')) {
      if (href.indexOf('single-level') > 0) {
        if (href.indexOf('regular') > 0) {
          listOfFiles.push(baseUrl + href)
        }
      }
      continue
    }

    await crawlListOfGrib2FilePaths(baseUrl + href, listOfFiles)
  }

  return listOfFiles
}

exports.crawlListOfGrib2FilePaths = crawlListOfGrib2FilePaths
exports.crawlListOfFilePaths = crawlListOfFilePaths
