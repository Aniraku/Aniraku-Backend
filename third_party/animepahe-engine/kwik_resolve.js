// kwik_resolve.js — one-shot sandbox for kwik.cx player scripts (animepahe).
//
// stdin : the page's obfuscated <script> body (extracted by Go).
// stdout: every captured eval/console payload, one per line; Go greps the
//         first line containing ".m3u8".
//
// The kwik player decodes its m3u8 URL by eval()-ing an obfuscated string.
// Instead of reimplementing the decoder (it changes), we execute the page's
// own script with eval and console hooked — the URL appears verbatim in a
// captured payload. This is the exact wrapper proven end-to-end against a
// live kwik page (Naruto ep1 -> vault-01.uwucdn.top/.../uwu.m3u8 -> 200).
//
// Sandbox hardening: the script only decodes strings, so every capability it
// could abuse is stubbed out. Network is dead (fetch/XHR/WebSocket), the DOM
// is inert, and `process` is removed after we take the handles we need — a
// script that touches it fails loudly into the captured output instead of
// touching the host.
"use strict";

const fs = require("fs");
const hostProcess = process;

const src = fs.readFileSync(0, "utf8");
if (!src.trim()) {
  hostProcess.exit(2);
}

const captured = [];
const exit = (code) => hostProcess.exit(code);
const write = (line) => hostProcess.stdout.write(line + "\n");
const flushAndExit = () => {
  for (const line of captured) {
    write(String(line).replace(/\r?\n/g, "\\n"));
  }
  exit(0);
};

globalThis.window = { location: { href: "" } };
globalThis.document = {
  cookie: "",
  getElementById: () => null,
  querySelector: () => null,
  querySelectorAll: () => [],
  createElement: () => ({ style: {}, setAttribute() {}, appendChild() {} }),
  addEventListener() {},
};
globalThis.navigator = { userAgent: "mozilla", language: "en-US" };
globalThis.fetch = () => Promise.reject(new Error("sandbox: network disabled"));
globalThis.XMLHttpRequest = class {
  open() {}
  send() {}
  setRequestHeader() {}
  addEventListener() {}
};
globalThis.WebSocket = class {
  constructor() {
    throw new Error("sandbox: network disabled");
  }
};
globalThis.importScripts = () => {
  throw new Error("sandbox: workers disabled");
};

console.log = (...args) => {
  captured.push(args.join(" "));
};
const origEval = globalThis.eval;
globalThis.eval = (x) => {
  captured.push("[EVAL]" + x);
  return origEval(x);
};

try {
  // Indirect eval: runs the page script in global (sloppy) scope, exactly
  // like the browser would, while `eval` inside it resolves to our hook.
  origEval(src);
} catch (e) {
  captured.push("[ERR]" + (e && e.message ? e.message : e));
}

// Drop the Node/bun host handles before the script's deferred callbacks run
// (we already captured the closures we need above). The page script must
// never reach require/process: it only decodes strings.
try {
  delete globalThis.process;
  delete globalThis.require;
} catch (_) {
  /* best effort */
}

setTimeout(flushAndExit, 300);
