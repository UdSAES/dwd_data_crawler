// One-off admin process to delete grib2-files in rotated coordinates
// SPDX-License-Identifier: MIT

'use strict'

const _ = require('lodash')
const fs = require('fs-extra')
const path = require('path')
const processenv = require('processenv')
const bunyan = require('bunyan')

// Load configuration
const DOWNLOAD_DIRECTORY_BASE_PATH = processenv('DOWNLOAD_DIRECTORY_BASE_PATH')
const ROTATED_NEW_DIRECTORY_BASE_PATH = processenv('ROTATED_NEW_DIRECTORY_BASE_PATH')

// Instantiate logger
let log = bunyan.createLogger({
  name: 'grib2_rotated_delete.js',
  level: 'debug'
})
log.info('entering admin script `grib2_rotated_delete.js`')

// Check validity of inputs
async function checkIfConfigIsValid () {
  if (_.isNil(DOWNLOAD_DIRECTORY_BASE_PATH)) {
    log.fatal('FATAL: environment variable DOWNLOAD_DIRECTORY_BASE_PATH missing')
    process.exit(1)
  } else if (!(await fs.pathExists(DOWNLOAD_DIRECTORY_BASE_PATH))) {
    log.fatal('FATAL: DOWNLOAD_DIRECTORY_BASE_PATH is given but does not exist')
    process.exit(1)
  } else if (_.isNil(ROTATED_NEW_DIRECTORY_BASE_PATH)) {
    log.fatal('FATAL: environment variable ROTATED_NEW_DIRECTORY_BASE_PATH missing')
    process.exit(1)
  } else if (!(await fs.pathExists(ROTATED_NEW_DIRECTORY_BASE_PATH))) {
    log.fatal('FATAL: ROTATED_NEW_DIRECTORY_BASE_PATH is given but does not exist')
    process.exit(1)
  } else {
    log.info('DOWNLOAD_DIRECTORY_BASE_PATH is set to', DOWNLOAD_DIRECTORY_BASE_PATH)
    log.info('ROTATED_NEW_DIRECTORY_BASE_PATH is set to', ROTATED_NEW_DIRECTORY_BASE_PATH)
  }
}

// Define functions
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
          if (
            (_.includes(file, 'rotated') === true) &&
            (_.endsWith(file, 'grib2.lz4'))
          ) {
            const sibling = await _.replace(file, 'rotated', 'regular')
            const fileHasSibling = await fs.pathExists(
              path.join(subSubDirPath, sibling)
            )
            if (fileHasSibling === true) {
              log.debug(`file ${file} has sibling ${sibling}`)

              // Move the rotated file to a separate directory
              const filePathOld = filePath
              const filePathNew = await _.replace(
                filePathOld,
                basePathOld,
                basePathNew
              )
              await fs.move(filePathOld, filePathNew)
              numberOfFilesMoved += 1

              log.debug(`moved ${filePathOld} to ${filePathNew}`)
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

// Define main function
const main = async function () {
  await checkIfConfigIsValid()

  const gribFilesBasePath = path.join(
    DOWNLOAD_DIRECTORY_BASE_PATH,
    'weather',
    'cosmo-d2',
    'grib'
  )
  const gribFilesBasePathExists = await fs.pathExists(gribFilesBasePath)

  const rotatedFilesBasePath = path.join(
    ROTATED_NEW_DIRECTORY_BASE_PATH,
    'cosmo-d2',
    'grib'
  )

  let totalFilesMoved = 0

  if (gribFilesBasePathExists) {
    try {
      await fs.ensureDir(rotatedFilesBasePath)
      totalFilesMoved = await moveAllRotatedGrib2Files(gribFilesBasePath, rotatedFilesBasePath)
    } catch (error) {
      log.fatal(error)
      process.exit(1)
    }
    log.info(`successfully moved ${totalFilesMoved} files!`)
  }
}

// Execute as independent script
if (require.main === module) {
  main()
}
