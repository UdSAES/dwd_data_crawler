// One-off admin process to apply actions to specific files, e.g.
// -- delete grib2-files in rotated coordinates
// -- move oldest files to separate harddisk in order to gain space
// SPDX-License-Identifier: MIT

'use strict'

const fs = require('fs')
const process = require('process')
const path = require('path')
const Minio = require('minio')
const { Client } = require('@elastic/elasticsearch')

// https://docs.min.io/docs/javascript-client-api-reference.html#putObject

const ACCESS_KEY = process.env.ACCESS_KEY
const SECRET_KEY = process.env.SECRET_KEY
const PATH_TO_STORAGE = process.env.PATH_TO_STORAGE
const MINIO_ENDPOINT_URI = process.env.MINIO_ENDPOINT_URI
const MINIO_PORT = process.env.MINIO_PORT
const ELASTICSEARCH_ENDPOINT_URI = process.env.ELASTICSEARCH_ENDPOINT_URI
const ELASTICSEARCH_PORT = process.env.ELASTICSEARCH_PORT

// Initialize minio client on PORT: 9000. Should be the same port docker is using.
const minioClient = new Minio.Client({
  endPoint: MINIO_ENDPOINT_URI,
  port: parseInt(MINIO_PORT),
  useSSL: false,
  accessKey: ACCESS_KEY,
  secretKey: SECRET_KEY
})

// Just some metadata, can be different.
const metaData = {
  'Content-Type': 'application/octet-stream',
  'X-Amz-Meta-Testing': 1234,
  example: 5678,
  'Model-Type': 'COSMO-D2',
  'Model-Run': '2020-08-19'
}

// Get files
const files = []

fs.readdirSync(PATH_TO_STORAGE).forEach(file => {
  files.push(path.join(PATH_TO_STORAGE, file))
})

function makeBucket (bucketName, region = 'eu-east-1') {
  // Create bucket
  const result = new Promise((resolve, reject) => {
    minioClient.makeBucket(bucketName, region, function (err) {
      if (!err) {
        return resolve(`Bucket ${bucketName} created successfully in ${region}.`)
      } else {
        return reject(err)
      }
    })
  })
  console.log('')
  return result
}

function uploadFilesToBucket (bucketName, files, metaData) {
  const result = new Promise((resolve, reject) => {
    // upload each file
    for (let i = 0; i < files.length; i += 1) {
      metaData.object_id = i
      minioClient.fPutObject(bucketName, `file${i}`, files[i], metaData, function (err, etag) {
        if (!err) {
          return resolve(`File ${files[i]} was uploaded to bucket ${bucketName}`)
        } else {
          return reject(err, 'Did not upload file')
        }
      })
    }
  })
  return result
}

async function runIndexing (bucketName) {
  // List each file in a bucket

  var stream = minioClient.extensions.listObjectsV2WithMetadata(bucketName, '', true, '')

  // https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/api-reference.html
  const client = new Client({ node: `${ELASTICSEARCH_ENDPOINT_URI}:${ELASTICSEARCH_PORT}` })

  // Listing items returns Readable stream, and each object in indexed
  stream.on('data', async function (obj) {
    const data = {
      index: 'test1337',
      refresh: true,
      body: obj
    }
    await client.index(data)
    console.log(data)
  })
  // Or if error: show error
  stream.on('error', function (err) { console.log(err) })
}

(async () => {
  const bucketName = 'udsaes'
  makeBucket(bucketName).then((res) => {
    // Only after creating a bucket
    console.log(res)
    // Upload files
    uploadFilesToBucket(bucketName, files, metaData).then((res) => {
      console.log(res)
      // Only after uploading files to the bucket, index them
      console.log('RUNNING INDEXING')
      runIndexing(bucketName)
    }).catch((err) => {
      console.log(err)
      console.log('Files were not uploaded')
    })
  }).catch((err) => { console.log(err) })
})()
