const fs = require('fs')
const process = require('process')
const path = require('path')
const Minio = require('minio')


// Instantiate the minio client with the endpoint
// and access keys as shown below.

const ACCESS_KEY = process.env.ACCESS_KEY
const SECRET_KEY = process.env.SECRET_KEY
const PATH_TO_STORAGE = process.env.PATH_TO_STORAGE
const ENDPOINT_URI = process.env.ENDPOINT_URI
const PORT = process.env.PORT

// Something is weird with the port. It could not load it from .env

const minioClient = new Minio.Client({
    endPoint: ENDPOINT_URI,
    port: 9000,
    useSSL: false,
    accessKey: ACCESS_KEY,
    secretKey: SECRET_KEY,
});

const metaData = {
    'Content-Type': 'application/octet-stream',
    'X-Amz-Meta-Testing': 1234,
    'example': 5678,
    'Model-Type': 'COSMO-D2',
    'Model-Run': '2020-08-19'
}

console.log('Initialized client')


const bucketName = 'mybucket'
const region = 'us-east-1'

async function putFileToBucket(bucketName, filenameInBucket, file, metaData) {
    await minioClient.fPutObject(bucketName, filenameInBucket, file, metaData, function(err, etag) {
        if (err) return console.log(err)
            console.log('File uploaded successfully.')
    });
}

// Create bucket
minioClient.makeBucket(bucketName, region, function(err) {
  if (err) return console.log('Error creating bucket.', err)
  console.log(`Bucket ${bucketName} created successfully in ${region}.`)
})

const files = []

fs.readdirSync(PATH_TO_STORAGE).forEach(file => {
    files.push(path.join(PATH_TO_STORAGE, file))
})


for (let i = 0; i < files.length; i += 1) {
    minioClient.fPutObject(bucketName, `file${i}`, files[i], metaData, function(err, etag) {
    if (err) return console.log(err)
        console.log('File uploaded successfully.')
    })
}
