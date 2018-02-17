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

const _ = require('lodash')
const assert = require('assert')
const moment = require('moment')
function parseCSV(fileContent) {
  assert(_.isString(fileContent))
  fileContent = fileContent.replace(/\r\n/g, '\n')
  const lineStrings = fileContent.split('\n')

  const lines = _.map(lineStrings, (lineString) => {
    return lineString.split(';')
  })
  return lines
}

function generateCSV(table) {
  const lines = _.map(table, (line) => {
    return line.join(';')
  })

  return lines.join('\r\n')
}

function mergeCSVContents(fileContent1, fileContent2, dateString) {
  assert(_.isString(fileContent1))
  assert(_.isString(fileContent2))
  assert(_.isString(dateString))


  const table1 = parseCSV(fileContent1)
  const table2 = parseCSV(fileContent2)

  if (table1.length < 3) {
    throw new Error("table1.length < 3")
  }

  if (table2.length < 3) {
    throw new Error("table2.length < 3")
  }

  for (var i = 0; i < 3; i++) {
    if (!_.isEqual(table1[i], table2[i])) {
      throw new Error("different table headings")
    }
  }


  for (var i = 3; i < table2.length; i++) {
    const table2Row = table2[i]
    const table1Row = _.find(table1, (table1Row) => {
      return _.isEqual(table1Row, table2Row)
    })

    if (_.isNil(table1Row)) {
      table1.push(table2Row)
    }
  }

  // get rid of all lines which are too short
  _.remove(table1, (line) => {
    return line.length < 2
  })

  // get of all lines which do not have a valid date
  _.remove(table1, (line, index) => {
    if (index < 3) {
      return false
    }

    const dateString = line[0]
    const timeString = line[1]

    const m = moment(dateString + ' ' + timeString, "DD.MM.YY HH:mm")
    return !m.isValid()
  })

  // get rid of all lines which have a wrong date string
  _.remove(table1, (line, index) => {
    if (index < 3) {
      return false
    }
    //console.log('%s vs. %s', line[0], dateString)
    return line[0] !== dateString
  })

  var valueLines = table1.slice(3)
  valueLines = _.sortBy(valueLines, (line) => {
    const dateString = line[0]
    const timeString = line[1]

    const m = moment(dateString + ' ' + timeString, "DD.MM.YY HH:mm")

    return m.valueOf()
  })

  _.reverse(valueLines)


  var result = _.concat(table1.slice(0,3), valueLines)
  return generateCSV(result)
}

exports.parseCSV = parseCSV
exports.generateCSV = generateCSV
exports.mergeCSVContents = mergeCSVContents
