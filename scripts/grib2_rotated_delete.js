// One-off admin process to delete grib2-files in rotated coordinates
// SPDX-License-Identifier: MIT

// TODO replace plain console.log-entries by structured ones

'use strict'

const _ = require('lodash')
const fs = require('fs-extra')
const path = require('path')
const processenv = require('processenv')

// Load configuration
const DOWNLOAD_DIRECTORY_BASE_PATH = processenv('DOWNLOAD_DIRECTORY_BASE_PATH')
const ROTATED_NEW_DIRECTORY_BASE_PATH = processenv('ROTATED_NEW_DIRECTORY_BASE_PATH')

// Check validity of inputs
async function checkIfConfigIsValid () {
  if (_.isNil(DOWNLOAD_DIRECTORY_BASE_PATH)) {
    console.log('FATAL: environment variable DOWNLOAD_DIRECTORY_BASE_PATH missing')
    process.exit(1)
  } else if (!(await fs.pathExists(DOWNLOAD_DIRECTORY_BASE_PATH))) {
    console.log('FATAL: DOWNLOAD_DIRECTORY_BASE_PATH is given but does not exist')
    process.exit(1)
  } else if (_.isNil(ROTATED_NEW_DIRECTORY_BASE_PATH)) {
    console.log('FATAL: environment variable ROTATED_NEW_DIRECTORY_BASE_PATH missing')
    process.exit(1)
  } else if (!(await fs.pathExists(ROTATED_NEW_DIRECTORY_BASE_PATH))) {
    console.log('FATAL: ROTATED_NEW_DIRECTORY_BASE_PATH is given but does not exist')
    process.exit(1)
  } else {
    console.log('DOWNLOAD_DIRECTORY_BASE_PATH is set to', DOWNLOAD_DIRECTORY_BASE_PATH)
    console.log('ROTATED_NEW_DIRECTORY_BASE_PATH is set to', ROTATED_NEW_DIRECTORY_BASE_PATH)
  }
}

// Define functions
async function findAllRotatedGrib2Files (basePath) {
  const list = []
  try {
    const subDirs = await fs.readdir(basePath)
    for (const subDir of subDirs) {
      const subDirPath = path.join(basePath, subDir)

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
            // console.log(file, fileHasSibling, sibling)
            if (fileHasSibling === true) {
              list.push(filePath)
            }
          }
        }
      }
    }
  } catch (error) {
    console.log(error)
    process.exit(1)
  }
  return list
}

// Define main function
const main = async function () {
  await checkIfConfigIsValid()

  // Find all grib2-files that have 'rotated' as part of their filename and have a 'regular' sibling
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
  await fs.ensureDir(rotatedFilesBasePath)

  if (gribFilesBasePathExists) {
    try {
      const listOfRotatedGrib2Files = await findAllRotatedGrib2Files(gribFilesBasePath)
      console.log(
        'listOfRotatedGrib2Files:', listOfRotatedGrib2Files
      )

      // Move the rotated files to a separate directory
      for (const filePathOld of listOfRotatedGrib2Files) {
        const filePathNew = await _.replace(
          filePathOld,
          gribFilesBasePath,
          rotatedFilesBasePath
        )
        await fs.move(filePathOld, filePathNew)
        console.log('\n')
        console.log(filePathOld)
        console.log(filePathNew)
      }
    } catch (error) {
      console.log(error)
      process.exit(1)
    }
  }
}

// Execute as independent script
if (require.main === module) {
  main()
}
