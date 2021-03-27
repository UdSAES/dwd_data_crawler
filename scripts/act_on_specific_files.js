// SPDX-FileCopyrightText: 2018 UdS AES <https://www.uni-saarland.de/lehrstuhl/frey.html>
// SPDX-License-Identifier: MIT

// One-off admin process to apply actions to specific files, e.g.
// -- delete grib2-files in rotated coordinates
// -- move oldest files to separate harddisk in order to gain space
// -- build an index of all files using Elasticsearch 7.x.x

'use strict'

const _ = require('lodash')
const fs = require('fs-extra')
const path = require('path')
const { processenv } = require('processenv')
const bunyan = require('bunyan')
const moment = require('moment')
const { Client } = require('@elastic/elasticsearch')

// Load configuration
const DOWNLOAD_DIRECTORY_BASE_PATH = processenv('DOWNLOAD_DIRECTORY_BASE_PATH')
const NEW_DIRECTORY_BASE_PATH = processenv('NEW_DIRECTORY_BASE_PATH')
const CRITERION = processenv('CRITERION')
const THRESHOLD = processenv('THRESHOLD')
const ELASTICSEARCH_ORIGIN = processenv('ELASTICSEARCH_ORIGIN')
const INDEX_NAME_PREFIX = processenv('INDEX_NAME_PREFIX')
const INDEX_FROM_SCRATCH = processenv('INDEX_FROM_SCRATCH')

// Instantiate logger
const log = bunyan.createLogger({
  name: 'act_on_specific_files.js',
  level: 'debug'
})
log.info('entering admin script `act_on_specific_files.js`')

// Define validity of inputs
async function checkIfConfigIsValid () {
  if (_.isNil(DOWNLOAD_DIRECTORY_BASE_PATH)) {
    log.fatal('FATAL: environment variable DOWNLOAD_DIRECTORY_BASE_PATH missing')
    process.exit(1)
  } else if (!(await fs.pathExists(DOWNLOAD_DIRECTORY_BASE_PATH))) {
    log.fatal('FATAL: DOWNLOAD_DIRECTORY_BASE_PATH is given but does not exist')
    process.exit(1)
  } else if (_.isNil(NEW_DIRECTORY_BASE_PATH)) {
    log.fatal('FATAL: environment variable NEW_DIRECTORY_BASE_PATH missing')
    process.exit(1)
  } else if (!(await fs.pathExists(NEW_DIRECTORY_BASE_PATH))) {
    log.fatal('FATAL: NEW_DIRECTORY_BASE_PATH is given but does not exist')
    process.exit(1)
  } else if (
    _.isNil(CRITERION) ||
    !(CRITERION === 'rotated' || CRITERION === 'oldest' || CRITERION === 'index')
  ) {
    log.fatal(
      'FATAL: environment variable CRITERION missing or is not set to "rotated" or "oldest" or "index"'
    )
    process.exit(1)
  } else if (!_.isString(THRESHOLD)) {
    log.fatal('FATAL: environment variable THRESHOLD missing or not a string')
    process.exit(1)
  } else if (_.isNil(ELASTICSEARCH_ORIGIN)) {
    log.fatal('FATAL: environment variable ELASTICSEARCH_ORIGIN missing')
    process.exit(1)
  } else if (_.isNil(INDEX_NAME_PREFIX)) {
    log.fatal('FATAL: environment variable INDEX_NAME_PREFIX missing')
    process.exit(1)
  } else if (_.isNil(INDEX_FROM_SCRATCH)) {
    log.fatal('FATAL: environment variable INDEX_FROM_SCRATCH missing')
    process.exit(1)
  } else {
    log.info('DOWNLOAD_DIRECTORY_BASE_PATH is set to', DOWNLOAD_DIRECTORY_BASE_PATH)
    log.info('NEW_DIRECTORY_BASE_PATH is set to', NEW_DIRECTORY_BASE_PATH)
    log.info('CRITERION is set to', CRITERION)
    log.info('THRESHOLD is set to', THRESHOLD)
    log.info('ELASTICSEARCH_ORIGIN is set to', ELASTICSEARCH_ORIGIN)
    log.info('INDEX_NAME_PREFIX is set to', INDEX_NAME_PREFIX)
    log.info('INDEX_FROM_SCRATCH is set to', INDEX_FROM_SCRATCH)
    log.info('configuration is valid, moving on...')
  }
}

// Define functions
async function applyActionToAllFilesMatchingCriteria (basePath, criterion, action) {
  log.debug(`traversing directory ${basePath}`)
  let numberOfFilesActedOn = 0

  let dirContents = null
  try {
    dirContents = await fs.readdir(basePath)
  } catch (error) {
    log.error(error)
    return
  }

  for (const item of dirContents) {
    const itemPath = path.join(basePath, item)
    const itemProperties = await fs.stat(itemPath)

    if (itemProperties.isDirectory()) {
      numberOfFilesActedOn += await applyActionToAllFilesMatchingCriteria(
        itemPath,
        criterion,
        action
      )
    } else {
      if (await criterion(itemPath)) {
        try {
          await action(itemPath)
        } catch (error) {
          log.error(error)
          continue
        }
        numberOfFilesActedOn += 1
      }
    }
  }
  return numberOfFilesActedOn
}

async function getFieldsBeob (itemPath) {}

async function getFieldsMosmix (itemPath) {}

async function getFieldsCosmoDe (itemPath) {
  const fileNameWithExtension = _.last(_.split(itemPath, path.sep))
  const fileNameDotSeparated = _.split(fileNameWithExtension, '.')
  const fileName = _.join(_.dropRight(fileNameDotSeparated, 2), '.')
  const fileExtension = _.join(
    _.drop(fileNameDotSeparated, fileNameDotSeparated.length - 2),
    '.'
  )
  const fileNameTokens = _.split(fileName, '_')
  const itemProperties = await fs.stat(itemPath)

  const run = moment.utc(_.nth(fileNameTokens, -2), 'YYYYMMDDHH')
  const step = _.nth(fileNameTokens, -1)
  const datetime = run.clone().add(_.parseInt(step), 'hours')

  const fields = {
    model: 'cosmo-de',
    scope: {
      temporal: {
        start: datetime.toISOString(),
        end: datetime.toISOString()
      },
      geographical: {
        region: 'germany'
      }
    },
    nwp: {
      gridType: '?', // FIXME
      levelType: _.join(_.slice(fileNameTokens, 1, 3), '-'),
      run: run.toISOString(),
      runOfDay: _.join(_.slice(_.nth(fileNameTokens, -2), 8), ''),
      step: step,
      level: '?', // FIXME
      field: _.toLower(_.join(_.drop(_.dropRight(fileNameTokens, 2), 4), '_'))
    },
    file: {
      path: itemPath,
      type: fileExtension,
      format: _.first(_.split(fileExtension, '.')),
      size: itemProperties.size
    }
  }

  return fields
}

async function getFieldsCosmoD2 (itemPath) {
  const fileNameWithExtension = _.last(_.split(itemPath, path.sep))
  const fileNameDotSeparated = _.split(fileNameWithExtension, '.')
  const fileName = _.join(_.dropRight(fileNameDotSeparated, 2), '.')
  const fileExtension = _.join(
    _.drop(fileNameDotSeparated, fileNameDotSeparated.length - 2),
    '.'
  )
  const fileNameTokens = _.split(fileName, '_')
  const itemProperties = await fs.stat(itemPath)

  const run = moment.utc(_.nth(fileNameTokens, 4), 'YYYYMMDDHH')
  const step = _.nth(fileNameTokens, 5)
  const datetime = run.clone().add(_.parseInt(step), 'hours')

  const fields = {
    model: _.nth(fileNameTokens, 0),
    scope: {
      temporal: {
        start: datetime.toISOString(),
        end: datetime.toISOString()
      },
      geographical: {
        region: _.nth(fileNameTokens, 1)
      }
    },
    nwp: {
      gridType: _.nth(fileNameTokens, 2),
      levelType: _.nth(fileNameTokens, 3),
      run: run.toISOString(),
      runOfDay: _.join(_.slice(_.nth(fileNameTokens, 4), 8), ''),
      step: step,
      level: '?', // FIXME
      field: _.toLower(_.join(_.drop(fileNameTokens, 6), '_'))
    },
    file: {
      path: itemPath,
      type: fileExtension,
      format: _.first(_.split(fileExtension, '.')),
      size: itemProperties.size
    }
  }

  return fields
}

async function getFieldsIconD2 (itemPath) {
  const fileNameWithExtension = _.last(_.split(itemPath, path.sep))
  const fileNameDotSeparated = _.split(fileNameWithExtension, '.')
  const fileName = _.join(_.dropRight(fileNameDotSeparated, 2), '.')
  const fileExtension = _.join(
    _.drop(fileNameDotSeparated, fileNameDotSeparated.length - 2),
    '.'
  )
  const fileNameTokens = _.split(fileName, '_')
  const itemProperties = await fs.stat(itemPath)

  const run = moment.utc(_.nth(fileNameTokens, 4), 'YYYYMMDDHH')
  const step = _.nth(fileNameTokens, 5)
  const datetime = run.clone().add(_.parseInt(step), 'hours')

  const fields = {
    model: _.nth(fileNameTokens, 0),
    scope: {
      temporal: {
        start: datetime.toISOString(),
        end: datetime.toISOString()
      },
      geographical: {
        region: _.nth(fileNameTokens, 1)
      }
    },
    nwp: {
      gridType: _.nth(fileNameTokens, 2),
      levelType: _.nth(fileNameTokens, 3),
      run: run.toISOString(),
      runOfDay: _.join(_.slice(_.nth(fileNameTokens, 4), 8), ''),
      step: step,
      level: _.nth(fileNameTokens, 6),
      field: _.join(_.drop(fileNameTokens, 7), '_')
    },
    file: {
      path: itemPath,
      type: fileExtension,
      format: _.first(_.split(fileExtension, '.')),
      size: itemProperties.size
    }
  }

  return fields
}

// Definition of functions to evaluate criteria

// async function isRotatedGrib2WithSibling (filePath) {
//   const fileName = path.basename(filePath)
//
//   if (_.includes(fileName, 'rotated') === true && _.endsWith(fileName, 'grib2.lz4')) {
//     const siblingName = await _.replace(fileName, 'rotated', 'regular')
//     const fileHasSibling = await fs.pathExists(
//       path.join(path.dirname(filePath), siblingName)
//     )
//     return fileHasSibling
//   } else {
//     return false
//   }
// }

// async function createdBeforeDate (filePath, dateStringIso8601) {
//   const stats = await fs.stat(filePath)
//   const fileBirthTime = moment(stats.birthtimeMs).utc()
//   const threshold = moment(dateStringIso8601).utc()
//
//   return moment(fileBirthTime).isBefore(threshold)
// }

async function filePathHasDateBeforeOrAfter (
  filePath,
  dateStringIso8601,
  beforeOrAfter
) {
  const regex = /^20[1-2]{1}[0-9]{1}[0-1]{1}[0-9]{3,5}$/
  const threshold = moment(dateStringIso8601).utc()
  const filePathParts = _.split(filePath, path.sep)
  const forecastRun = String(
    _.find(filePathParts, function (part) {
      return part.match(regex)
    })
  )
  let forecastRunAsObject = null
  if (forecastRun.length === 8) {
    forecastRunAsObject = moment.utc(forecastRun)
  } else if (forecastRun.length === 10) {
    forecastRunAsObject = moment.utc(forecastRun, 'YYYYMMDDHH')
  }

  if (beforeOrAfter === 'before') {
    return moment(forecastRunAsObject).isBefore(threshold)
  }
  if (beforeOrAfter === 'after') {
    return moment(forecastRunAsObject).isAfter(threshold)
  }
}

// Definition of actions
async function moveToNewBasedirKeepSubdirs (filePathOld, basePathOld, basePathNew) {
  const filePathNew = await _.replace(filePathOld, basePathOld, basePathNew)

  try {
    await fs.move(filePathOld, filePathNew)
    log.debug(`moved ${filePathOld} to ${filePathNew}`)
  } catch (error) {
    log.error(error)
  }
}

async function moveAllRotatedGrib2Files (basePathOld, basePathNew) {
  // Find all grib2-files that have 'rotated' as part of their filename
  // and have a 'regular' sibling, then replace its basePath/move it
  let numberOfFilesMoved = 0

  try {
    const subDirs = await fs.readdir(basePathOld)
    for (const subDir of subDirs) {
      const subDirPath = path.join(basePathOld, subDir)

      const subSubDirs = await fs.readdir(subDirPath)
      for (const subSubDir of subSubDirs) {
        const subSubDirPath = path.join(subDirPath, subSubDir)

        const files = await fs.readdir(subSubDirPath)
        for (const file of files) {
          // Iff there is a 'regular' version,  mark the 'rotated' one for removal
          const filePath = path.join(subSubDirPath, file)
          if (_.includes(file, 'rotated') === true && _.endsWith(file, 'grib2.lz4')) {
            const sibling = await _.replace(file, 'rotated', 'regular')
            const fileHasSibling = await fs.pathExists(
              path.join(subSubDirPath, sibling)
            )
            if (fileHasSibling === true) {
              log.debug(`file ${file} has sibling ${sibling}`)

              // Move the rotated file to a separate directory
              const filePathOld = filePath
              const filePathNew = await _.replace(filePathOld, basePathOld, basePathNew)

              try {
                await fs.move(filePathOld, filePathNew)
                numberOfFilesMoved += 1

                log.debug(`moved ${filePathOld} to ${filePathNew}`)
              } catch (error) {
                log.warn(error)
              }
            }
          }
        }
        log.debug(`analyzed ${files.length} files in ./${subDir}/${subSubDir}`)
      }
      log.info(`analyzed files in ./${subDir}`)
    }
  } catch (error) {
    log.fatal(error)
    process.exit(1)
  }
  return numberOfFilesMoved
}

async function indexFileInElasticsearch (client, index, filePath) {
  // Identify model type (BEOB/MOSMIX/COSMO-DE/COSMO-D2/ICON-D2)

  const fileName = _.last(_.split(filePath, path.sep))
  let modelType = ''
  _.forEach(['BEOB', 'MOSMIX', 'COSMODE', 'cosmo-d2', 'icon-d2'], (substring) => {
    if (_.includes(fileName, substring)) {
      modelType = _.toUpper(substring)
    }
  })

  // Fill in all fields
  let fields = {}
  switch (modelType) {
    case 'BEOB':
      fields = await getFieldsBeob(filePath)
      break
    case 'MOSMIX':
      fields = await getFieldsMosmix(filePath)
      break
    case 'COSMODE':
      fields = await getFieldsCosmoDe(filePath)
      break
    case 'COSMO-D2':
      fields = await getFieldsCosmoD2(filePath)
      break
    case 'ICON-D2':
      fields = await getFieldsIconD2(filePath)
      break
  }

  // Index in Elasticsearch
  // NOTE: would be better to do in bulk, but that's premature optimization (?)
  try {
    await client.index({
      index: index,
      body: fields
    })
  } catch (error) {
    log.error(error)
  }
}

// Define main functions
async function runIndexationOfFilesInElasticsearch () {
  const client = new Client({ node: ELASTICSEARCH_ORIGIN })
  const index = INDEX_NAME_PREFIX
  let actualThreshold = THRESHOLD

  // Specifiy functions to call
  const youngerThanEnvvarThreshold = async (filePath) => {
    const result = await filePathHasDateBeforeOrAfter(
      filePath,
      actualThreshold,
      'after'
    )
    return result
  }

  const indexFile = async (filePath) => {
    const result = await indexFileInElasticsearch(client, index, filePath)
    return result
  }

  // Check whether or not the index already exists
  let indexExists = await client.indices.exists({
    index: index
  })
  indexExists = indexExists.body

  // Ensure that the index exists and decide at which datetime indexing should start
  if (INDEX_FROM_SCRATCH === true) {
    log.info('building an index from scratch')
    if (indexExists === true) {
      log.debug('deleting existing index...')
      // Clean up and start fresh
      await client.indices.delete({
        index: index
      })
    }

    log.debug(`adding new index '${index}'`)
    await client.indices.create({
      index: index
    })
  } else {
    // Find most recent file in index
    if (indexExists === true) {
      const response = await client.search({
        index: index,
        body: {
          query: {
            match_all: {}
          },
          sort: {
            'scope.temporal.start': 'desc'
          },
          size: 1
        }
      })

      // Disregard THRESHOLD given via ENVVAR and start at latest model run in index
      actualThreshold = response.body.hits.hits[0]._source.scope.temporal.start
      log.info(`adding data after model run ${actualThreshold} to existing index...`)
    } else {
      await client.indices.create({
        index: index
      })
    }
  }

  // Iterate over files and apply action
  const totalFilesActedOn = await applyActionToAllFilesMatchingCriteria(
    DOWNLOAD_DIRECTORY_BASE_PATH,
    youngerThanEnvvarThreshold,
    indexFile
  )

  return totalFilesActedOn
}

// Actual entrypoint to this entire mess of functions
const main = async function () {
  await checkIfConfigIsValid()

  let totalFilesActedOn = 0

  // Select criteria for identifying relevant files
  log.info(`attempting to apply action to files according to CRITERION '${CRITERION}'`)
  switch (CRITERION) {
    case 'rotated':
      const gribFilesBasePath = path.join(
        DOWNLOAD_DIRECTORY_BASE_PATH,
        'weather',
        'cosmo-d2',
        'grib'
      )
      const gribFilesBasePathExists = await fs.pathExists(gribFilesBasePath)

      const rotatedFilesBasePath = path.join(
        NEW_DIRECTORY_BASE_PATH,
        'cosmo-d2',
        'grib'
      )

      if (gribFilesBasePathExists) {
        try {
          await fs.ensureDir(rotatedFilesBasePath)
          totalFilesActedOn = await moveAllRotatedGrib2Files(
            gribFilesBasePath,
            rotatedFilesBasePath
          )
        } catch (error) {
          log.fatal(error)
          process.exit(1)
        }
      }
      break
    case 'oldest':
      const olderThanEnvvarThreshold = async (filePath) => {
        const result = await filePathHasDateBeforeOrAfter(filePath, THRESHOLD, 'before')
        return result
      }

      const moveFilesAway = async (filePath) => {
        const result = await moveToNewBasedirKeepSubdirs(
          filePath,
          DOWNLOAD_DIRECTORY_BASE_PATH,
          NEW_DIRECTORY_BASE_PATH
        )
        return result
      }

      totalFilesActedOn = await applyActionToAllFilesMatchingCriteria(
        DOWNLOAD_DIRECTORY_BASE_PATH,
        olderThanEnvvarThreshold,
        moveFilesAway
      )
      break
    case 'index':
      totalFilesActedOn = await runIndexationOfFilesInElasticsearch()
      break
  }
  log.info(`successfully acted on ${totalFilesActedOn} files!`)
}

// Execute as independent script
if (require.main === module) {
  main()
}
