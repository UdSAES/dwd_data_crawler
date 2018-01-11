'use strict'

const {VError} = require('verror')
const request = require('request-promise-native')
const DWD_URL_forecast = 'https://opendata.dwd.de/weather/local_forecasts/poi/10708-MOSMIX.csv'
const DWD_URL_report = 'https://opendata.dwd.de/weather/weather_reports/poi/10708-BEOB.csv'
const DWD_URL_Prefix_forecast = 'https://opendata.dwd.de/weather/local_forecasts/poi/'
const DWD_URL_Prefix_report = 'https://opendata.dwd.de/weather/weather_reports/poi/'
const DWD_URL_Postfix_forecast = '-MOSMIX.csv'
const DWD_URL_Postfix_report = '-BEOB.csv'

function parseForeCastsData(resultRowString) {
  const columns = resultRowString.split(';')

  return {
    datetime: columns[0] + ' ' + columns[1],
    ap: columns[31],
    dp: columns[3],
    sd: columns[33],
    gh: columns[34],
    ccl: columns[28],
    cc: columns[29],
    cch: columns[28],
    rr: columns[18],
    rp: columns[21],
    dd: columns[8],
    ff: columns[9]
  }
}

function parseReportsData(resultRowString) {
  const columns = resultRowString.split(';')

  return {
    datetime: columns[0] + ' ' + columns[1],
    ap: columns[36],
    dp: columns[5],
    rh: columns[37],
    sd: columns[42],
    gh: columns[7],
    dh: columns[6],
    bh: columns[8],
    cc: columns[2],
    rr: columns[30],
    tt: columns[39],
    dd: columns[22],
    ff: columns[23]
  }
}



async function getForeCastData(url) {
  try {
    var body = await request({
      method: 'get',
      url: url,
      simple: true
    })
  } catch (error) {
    throw error
  }

  const lines = body.split('\n')
  console.log(lines)
  return body
}

exports.getForeCastData = getForeCastData

getForeCastData(DWD_URL_report)
