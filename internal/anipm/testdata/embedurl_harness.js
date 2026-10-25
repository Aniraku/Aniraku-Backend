// Differential harness: runs the ORIGINAL ani.pm Player SDK
// (https://ani.pm/embed/sdk/v1.js) over a JSON array of option objects and
// prints a JSON array of results: {url} on success or {error:{code,message}} on
// failure.
//
//   node embedurl_harness.js <sdk.js path> <options.json>
//
// It is executed by differential_test.go; it is not part of the Go build. The
// SDK path is supplied by the test (a hash-verified local copy or a
// hash-verified fetch) so no third-party bundle is vendored in the repo.
'use strict'

const fs = require('fs')
const path = require('path')

const sdkPath = process.argv[2]
const file = process.argv[3]
if (!sdkPath || !file) {
  process.stderr.write('usage: node embedurl_harness.js <sdk.js> <options.json>\n')
  process.exit(2)
}

const sdk = require(path.resolve(sdkPath))
const cases = JSON.parse(fs.readFileSync(file, 'utf8'))
const out = cases.map((options) => {
  try {
    return { url: sdk.embedUrl(options) }
  } catch (err) {
    return { error: { code: err && err.code, message: err && err.message } }
  }
})
process.stdout.write(JSON.stringify(out))
