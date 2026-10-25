// Differential harness: runs the ORIGINAL ani.pm Player SDK
// (testdata/sdk-v1.js, fetched from https://ani.pm/embed/sdk/v1.js) over a JSON
// array of option objects and prints a JSON array of results: {url} on success
// or {error:{code,message}} on failure.
//
//   node embedurl_harness.js <options.json>
//
// It is executed by differential_test.go; it is not part of the Go build.
'use strict'

const fs = require('fs')
const path = require('path')
const sdk = require(path.join(__dirname, 'sdk-v1.js'))

const file = process.argv[2]
if (!file) {
  process.stderr.write('usage: node embedurl_harness.js <options.json>\n')
  process.exit(2)
}

const cases = JSON.parse(fs.readFileSync(file, 'utf8'))
const out = cases.map((options) => {
  try {
    return { url: sdk.embedUrl(options) }
  } catch (err) {
    return { error: { code: err && err.code, message: err && err.message } }
  }
})
process.stdout.write(JSON.stringify(out))
