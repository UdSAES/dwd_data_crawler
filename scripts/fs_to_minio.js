const fs = require('fs')
const process = require('process')
const path = require('path')
const Minio = require('minio')
const { Client } = require('@elastic/elasticsearch')

// https://docs.min.io/docs/javascript-client-api-reference.html#putObject

// Instantiate the minio client with the endpoint
// and access keys as shown below.

const ACCESS_KEY = process.env.ACCESS_KEY
const SECRET_KEY = process.env.SECRET_KEY
const PATH_TO_STORAGE = process.env.PATH_TO_STORAGE
const MINIO_ENDPOINT_URI = process.env.MINIO_ENDPOINT_URI
const MINIO_PORT = process.env.MINIO_PORT
const ELASTICSEARCH_ENDPOINT_URI = process.env.ELASTICSEARCH_ENDPOINT_URI
const ELASTICSEARCH_PORT = process.env.ELASTICSEARCH_PORT


//Initialize minio client on PORT: 9000. Should be the same port docker is using.
const minioClient = new Minio.Client({
    endPoint: MINIO_ENDPOINT_URI,
    port: parseInt(MINIO_PORT),
    useSSL: false,
    accessKey: ACCESS_KEY,
    secretKey: SECRET_KEY,
});

async function run_uploading() {

    // Just some metadata, can be different.
    const metaData = {
        'Content-Type': 'application/octet-stream',
        'X-Amz-Meta-Testing': 1234,
        'example': 5678,
        'Model-Type': 'COSMO-D2',
        'Model-Run': '2020-08-19'
    }

    console.log('Initialized client')


    // Initialize bucket. Once it is initialized execution of this code will not break anything
    // But it wil complain that bucket already exists. That is ok
    const bucketName = 'mybucket'
    const region = 'us-east-1'

    // Create bucket
    minioClient.makeBucket(bucketName, region, function(err) {
      if (err) return console.log('Error creating bucket.', err)
      console.log(`Bucket ${bucketName} created successfully in ${region}.`)
    })


    // Get full paths of the files
    const files = []

    fs.readdirSync(PATH_TO_STORAGE).forEach(file => {
        files.push(path.join(PATH_TO_STORAGE, file))
    })


    // Function that uploads the files
    async function putFileToBucket(bucketName, filenameInBucket, file, metaData) {
        await minioClient.fPutObject(bucketName, filenameInBucket, file, metaData, function(err, etag) {
            if (err) return console.log(err)
                console.log('File uploaded successfully.')
        });
    }


    // Upload files
    // In metaData any key can be, also object_id. Minio will use etag key nevertheless as identification.

    function uploadFilesToBucket(bucketName, files, metaData) {
        for (let i = 0; i < files.length; i += 1) {
            metaData.object_id = i
            minioClient.fPutObject(bucketName, `file${i}`, files[i], metaData, function(err, etag) {
                if (err) return console.log(err)
                    console.log('File uploaded successfully.')
                })
        }
    }

    uploadFilesToBucket('mybucket', files, metaData)

}

run_uploading().catch(console.log())

async function run_indexing() {

    var stream = minioClient.extensions.listObjectsV2WithMetadata('mybucket','', true,'')
    const hostAndPort = `${ELASTICSEARCH_ENDPOINT_URI}:${ELASTICSEARCH_PORT}`
    const client = new Client({ node: `${ELASTICSEARCH_ENDPOINT_URI}:${ELASTICSEARCH_PORT}` })

    stream.on('data', async function(obj) {
        const data = {
            index: 'test1337',
            refresh: true,
            body: obj
        }
        await client.index(data);
        console.log(data)
    })
    stream.on('error', function(err) { console.log(err) } )


    const { body } = await client.search({
    index: 'test1337',
    body: {
      query: {
        match: {
          name: 'file3'
        }
      }
    }
  })
  console.log(body.hits.hits)
}

run_indexing().catch(console.log)
