const sinon = require('sinon')
const t = require('tap')
const { PassThrough } = require('stream')

const proxyquire = require('proxyquire')
const s3Stub = {
  send: sinon.stub()
}
const s3Files = proxyquire('../s3-files.js', {
  '@aws-sdk/client-s3': {
    S3Client: sinon.stub().returns(s3Stub),
    GetObjectCommand: sinon.stub()
  }
})

// Connect
t.type(s3Files.s3, undefined)
s3Files.connect({})
t.type(s3Files.s3, 'object')

// Keystream
const keyStream = s3Files.createKeyStream('folder', undefined)
t.same(keyStream, null)

t.test('Connect without providing s3', function (child) {
  s3Files.connect({ region: 'some-region' })
  child.type(s3Files.s3, 'object')
  child.end()
})

t.test('keyStream', function (child) {
  const keyStream = s3Files.createKeyStream('folder/', ['a', 'b'])
  let cnt = 0
  keyStream.on('data', function (chunk) {
    if (cnt === 0) child.equal(chunk.toString(), 'folder/a')
    if (cnt === 1) child.equal(chunk.toString(), 'folder/b')
    cnt++
  })
  keyStream.on('end', function () {
    child.end()
  })
})

t.test('keyStream without folder having trailing slash', function (child) {
  const keyStream = s3Files.createKeyStream('folder', ['a', 'b'])
  let cnt = 0
  keyStream.on('data', function (chunk) {
    if (cnt === 0) child.equal(chunk.toString(), 'folder/a')
    if (cnt === 1) child.equal(chunk.toString(), 'folder/b')
    cnt++
  })
  keyStream.on('end', function () {
    child.end()
  })
})

t.test('keyStream without folder', function (child) {
  const keyStream = s3Files.createKeyStream('', ['a', 'b'])
  let cnt = 0
  keyStream.on('data', function (chunk) {
    if (cnt === 0) child.equal(chunk.toString(), 'a')
    if (cnt === 1) child.equal(chunk.toString(), 'b')
    cnt++
  })
  keyStream.on('end', function () {
    child.end()
  })
})

// Filestream
t.test('Filestream needs a bucket', async function (child) {
  child.plan(9) // Plan for 9 assertions
  let fileStream = await s3Files.createFileStream()
  child.same(fileStream, null)

  s3Files.connect({ bucket: 'bucket' })
  const keyStream = s3Files.createKeyStream('folder/', ['a', 'b', 'c'])

  const s = new PassThrough()
  s.end('hi')

  const readStream = {
    Body: s
  }
  s3Stub.send.resolves(readStream)

  let cnt = 0
  fileStream = await s3Files.createFileStream(keyStream)
  fileStream.on('data', function (chunk) {
    child.equal(chunk.data.toString(), 'hi')
    if (cnt === 0) child.equal(chunk.path, 'a')
    if (cnt === 1) {
      child.equal(chunk.path, 'b')
    }
    if (cnt === 2) {
      s.emit('error', new Error('fail'))
    }
    cnt++
  })
  fileStream.on('error', function (chunk) {
    child.ok(chunk)
  })
  fileStream.on('end', function (chunk) {
    child.end()
  })
})

t.end()
