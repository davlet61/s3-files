const { Readable } = require('stream')
const streamify = require('stream-array')
const concat = require('concat-stream')
const path = require('path')
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3')

const s3Files = {}
module.exports = s3Files

s3Files.connect = function (opts) {
  if ('s3' in opts) {
    this.s3 = opts.s3
  } else {
    const s3Client = new S3Client({
      region: opts.region
    })
    this.s3 = s3Client
  }
  this.bucket = opts.bucket
  return this
}

s3Files.createKeyStream = function (folder, keys) {
  if (!keys) return null
  const paths = keys.map((key) =>
    folder ? path.posix.join(folder, key) : key
  )
  return streamify(paths)
}

s3Files.createFileStream = async function (keyStream, preserveFolderPath) {
  if (!this.bucket) return null

  const rs = new Readable({ objectMode: true })

  let fileCounter = 0
  keyStream.on('data', async (file) => {
    fileCounter += 1
    if (fileCounter > 5) {
      keyStream.pause()
    }

    const params = { Bucket: this.bucket, Key: file }
    try {
      const s3FileStream = await this.s3.send(new GetObjectCommand(params))
      const s3File = s3FileStream.Body

      s3File.pipe(
        concat((buffer) => {
          const filePath = preserveFolderPath
            ? file
            : file.replace(/^.*[\\/]/, '')
          rs.push({ data: buffer, path: filePath })
        })
      )

      s3File.on('end', () => {
        fileCounter -= 1
        if (keyStream.isPaused()) {
          keyStream.resume()
        }
        if (fileCounter < 1) {
          rs.push(null)
        }
      })
    } catch (err) {
      err.file = file
      rs.emit('error', err)
    }
  })

  return rs
}
