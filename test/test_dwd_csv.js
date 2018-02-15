'use strict'

const assert = require('assert')
const _ = require('lodash')
const dwd_csv = require('../lib/dwd_csv')

describe('./lib/dwd_csv.js', () => {
  describe('parseCSV()', () => {
    it('should return an array of array of strings', () => {
      const tableMaster = [
        ['h1', 'h2'],
        ['1', '2'],
        ['end']
      ]
      const tableString = 'h1;h2\r\n1;2\r\nend'

      const parsedTable = dwd_csv.parseCSV(tableString)
      assert(_.isEqual(parsedTable, tableMaster))
    })
  })

  describe('mergeCSVContents()', () => {
    it('should return a merged table as string', () => {
      const masterTable = [
        ['l1_1', 'l1_2'],
        ['l1_1', 'l1_2'],
        ['l1_1', 'l1_2'],
        ['01.01.2018', '00:00'],
        ['01.01.2018', '01:00'],
        ['01.01.2018', '02:00'],
        ['01.01.2018', '03:00']
      ]

      const table1 = [
        ['l1_1', 'l1_2'],
        ['l1_1', 'l1_2'],
        ['l1_1', 'l1_2'],
        ['01.01.2018', '00:00'],
        ['01.01.2018', '03:00']
      ]
      const table1String = dwd_csv.generateCSV(table1)

      const table2 = [
        ['l1_1', 'l1_2'],
        ['l1_1', 'l1_2'],
        ['l1_1', 'l1_2'],
        ['01.01.2018', '01:00'],
        ['02.01.2018', '03:00'],
        ['01.01.2018', '02:00']
      ]
      const table2String = dwd_csv.generateCSV(table2)

      const mergedContent = dwd_csv.mergeCSVContents(table1String, table2String, masterTable[3][0])
      const mergedTable = dwd_csv.parseCSV(mergedContent)

      assert(_.isEqual(mergedTable, masterTable))
    })
  })
})
