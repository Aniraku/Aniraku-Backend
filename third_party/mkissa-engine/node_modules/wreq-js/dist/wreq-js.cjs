Object.defineProperties(exports, {
	__esModule: { value: true },
	[Symbol.toStringTag]: { value: "Module" }
});
let node_crypto = require("node:crypto");
let node_http = require("node:http");
let node_stream = require("node:stream");
let node_stream_web = require("node:stream/web");
//#region src/generated-types.ts
/**
* Newest profile per browser family, resolved when this file was generated.
* These move as new profiles land upstream, so diff this map when upgrading.
*/
const BROWSER_ALIASES = {
	chrome: "chrome_149",
	edge: "edge_148",
	firefox: "firefox_151",
	firefox_android: "firefox_android_135",
	firefox_private: "firefox_private_136",
	okhttp: "okhttp_5",
	opera: "opera_131",
	safari: "safari_26.4",
	safari_ios: "safari_ios_26.2",
	safari_ipad: "safari_ipad_26.2"
};
//#endregion
//#region src/native-require-cjs.ts
const nativeRequire = require;
//#endregion
//#region src/types.ts
/**
* Error thrown when a request fails. This can occur due to network errors,
* timeouts, invalid URLs, or other request-related issues.
*
* @example
* ```typescript
* try {
*   const response = await fetch('https://api.example.com');
* } catch (error) {
*   if (error instanceof RequestError) {
*     console.error('Request failed:', error.message);
*   }
* }
* ```
*/
var RequestError = class extends TypeError {
	constructor(message) {
		super(message);
		this.name = "RequestError";
	}
};
//#endregion
//#region src/wreq-js.ts
let nativeBinding;
let cachedProfiles;
let cachedProfileSet;
let cachedOperatingSystems;
let cachedOperatingSystemSet;
const cachedEmulationHeaders = /* @__PURE__ */ new Map();
function detectLibc() {
	if (process.platform !== "linux") return;
	const envLibc = process.env.LIBC ?? process.env.npm_config_libc;
	if (envLibc) return envLibc.toLowerCase().includes("musl") ? "musl" : "gnu";
	try {
		if ((process.report?.getReport?.())?.header?.glibcVersionRuntime) return "gnu";
		return "musl";
	} catch {
		return "gnu";
	}
}
const NATIVE_PLATFORMS = [
	"darwin-x64",
	"darwin-arm64",
	"linux-x64-gnu",
	"linux-x64-musl",
	"linux-arm64-gnu",
	"linux-arm64-musl",
	"win32-x64-msvc",
	"win32-arm64-msvc",
	"android-arm64"
];
function resolveNativePlatform() {
	const platform = process.platform;
	const arch = process.arch;
	if (platform === "android" && arch === "arm64") return "android-arm64";
	if (platform === "darwin" && arch === "x64") return "darwin-x64";
	if (platform === "darwin" && arch === "arm64") return "darwin-arm64";
	if (platform === "linux" && arch === "x64") return detectLibc() === "musl" ? "linux-x64-musl" : "linux-x64-gnu";
	if (platform === "linux" && arch === "arm64") return detectLibc() === "musl" ? "linux-arm64-musl" : "linux-arm64-gnu";
	if (platform === "win32" && arch === "x64") return "win32-x64-msvc";
	if (platform === "win32" && arch === "arm64") return "win32-arm64-msvc";
}
function loadNativeBinding() {
	const target = resolveNativePlatform();
	if (!target) {
		const libc = detectLibc();
		throw new Error(`Unsupported platform: ${process.platform}-${process.arch}${libc ? `-${libc}` : ""}. Supported platforms: ${NATIVE_PLATFORMS.join(", ")}`);
	}
	const candidates = [
		`../rust/wreq-js.${target}.node`,
		`@wreq-js/binding-${target}`,
		"../rust/wreq-js.node"
	];
	let notFoundError;
	let loadError;
	for (const candidate of candidates) try {
		return nativeRequire(candidate);
	} catch (error) {
		if (error?.code === "MODULE_NOT_FOUND") notFoundError ??= error;
		else loadError ??= error;
	}
	throw new Error(`Failed to load native module for ${target}. Tried: ${candidates.join(", ")}. Make sure the package is installed correctly and the native module is built for your platform.`, { cause: loadError ?? notFoundError });
}
nativeBinding = loadNativeBinding();
const websocketFinalizer = typeof FinalizationRegistry === "function" ? new FinalizationRegistry((connection) => {
	nativeBinding.websocketClose(connection).catch(() => void 0);
}) : void 0;
const bodyHandleFinalizer = typeof FinalizationRegistry === "function" ? new FinalizationRegistry((handle) => {
	if (handle.released) return;
	handle.released = true;
	try {
		nativeBinding.cancelBody(handle.id);
	} catch {}
}) : void 0;
const DEFAULT_BROWSER = BROWSER_ALIASES.chrome;
const DEFAULT_OS = "macos";
const DEFAULT_REQUEST_TIMEOUT_MS = 3e5;
const DEFAULT_TRUST_STORE = "combined";
const SUPPORTED_OSES = [
	"windows",
	"macos",
	"linux",
	"android",
	"ios"
];
const UTF8_DECODER = new TextDecoder("utf-8");
let ephemeralIdCounter = 0;
function generateEphemeralSessionId() {
	return `_e${++ephemeralIdCounter}`;
}
function generateSessionId() {
	return (0, node_crypto.randomUUID)();
}
function normalizeSessionOptions(options) {
	const sessionId = options?.sessionId ?? generateSessionId();
	const defaults = {
		transportMode: resolveEmulationMode(options?.browser, options?.os, options?.emulation),
		trustStore: DEFAULT_TRUST_STORE
	};
	if (options?.proxy !== void 0) defaults.proxy = options.proxy;
	if (options?.timeout !== void 0) {
		validateTimeout(options.timeout);
		defaults.timeout = options.timeout;
	}
	if (options?.insecure !== void 0) defaults.insecure = options.insecure;
	if (options?.trustStore !== void 0) {
		validateTrustStore(options.trustStore);
		defaults.trustStore = options.trustStore;
	}
	if (options?.defaultHeaders !== void 0) defaults.defaultHeaders = headersToTuples(options.defaultHeaders);
	if (options?.captureDiagnostics !== void 0) defaults.captureDiagnostics = options.captureDiagnostics;
	return {
		sessionId,
		defaults
	};
}
function isIterable(value) {
	return Boolean(value) && typeof value[Symbol.iterator] === "function";
}
function isPlainObject(value) {
	if (typeof value !== "object" || value === null) return false;
	const proto = Object.getPrototypeOf(value);
	return proto === Object.prototype || proto === null;
}
function coerceHeaderValue(value) {
	return String(value);
}
var Headers = class Headers {
	store = /* @__PURE__ */ new Map();
	constructor(init) {
		if (init) this.applyInit(init);
	}
	applyInit(init) {
		if (init instanceof Headers) {
			for (const [name, value] of init) this.append(name, value);
			return;
		}
		if (Array.isArray(init) || isIterable(init)) {
			for (const tuple of init) {
				if (!tuple) continue;
				const [name, value] = tuple;
				this.append(name, value);
			}
			return;
		}
		if (isPlainObject(init)) for (const [name, value] of Object.entries(init)) {
			if (value === void 0 || value === null) continue;
			this.set(name, coerceHeaderValue(value));
		}
	}
	normalizeName(name) {
		if (typeof name !== "string") throw new TypeError("Header name must be a string");
		const trimmed = name.trim();
		if (!trimmed) throw new TypeError("Header name must not be empty");
		return {
			key: trimmed.toLowerCase(),
			display: trimmed
		};
	}
	assertValue(value) {
		if (value === void 0 || value === null) throw new TypeError("Header value must not be null or undefined");
		return coerceHeaderValue(value);
	}
	append(name, value) {
		const normalized = this.normalizeName(name);
		const existing = this.store.get(normalized.key);
		const coercedValue = this.assertValue(value);
		if (existing) {
			existing.values.push(coercedValue);
			return;
		}
		this.store.set(normalized.key, {
			name: normalized.display,
			values: [coercedValue]
		});
	}
	set(name, value) {
		const normalized = this.normalizeName(name);
		const coercedValue = this.assertValue(value);
		this.store.set(normalized.key, {
			name: normalized.display,
			values: [coercedValue]
		});
	}
	get(name) {
		const normalized = this.normalizeName(name);
		const entry = this.store.get(normalized.key);
		return entry ? entry.values.join(", ") : null;
	}
	getSetCookie() {
		const entry = this.store.get("set-cookie");
		if (!entry) return [];
		return [...entry.values];
	}
	has(name) {
		const normalized = this.normalizeName(name);
		return this.store.has(normalized.key);
	}
	delete(name) {
		const normalized = this.normalizeName(name);
		this.store.delete(normalized.key);
	}
	entries() {
		return this[Symbol.iterator]();
	}
	*keys() {
		for (const [name] of this) yield name;
	}
	*values() {
		for (const [, value] of this) yield value;
	}
	forEach(callback, thisArg) {
		for (const [name, value] of this) callback.call(thisArg, value, name, this);
	}
	[Symbol.iterator]() {
		const generator = function* (store) {
			for (const entry of store.values()) if (entry.name.toLowerCase() === "set-cookie") for (const v of entry.values) yield [entry.name, v];
			else yield [entry.name, entry.values.join(", ")];
		};
		return generator(this.store);
	}
	toObject() {
		const result = {};
		for (const entry of this.store.values()) result[entry.name] = entry.values.join(", ");
		return result;
	}
	toTuples() {
		const result = [];
		for (const [name, value] of this) result.push([name, value]);
		return result;
	}
};
function headersToTuples(init) {
	return new Headers(init).toTuples();
}
function hasHeaderName(tuples, name) {
	if (!tuples) return false;
	const target = name.toLowerCase();
	for (const [headerName] of tuples) if (headerName.toLowerCase() === target) return true;
	return false;
}
function hasWebSocketProtocolHeader(headers) {
	const protocolHeaderName = "Sec-WebSocket-Protocol";
	if (!headers) return false;
	return hasHeaderName(headersToTuples(headers), protocolHeaderName);
}
function assertNoManualWebSocketProtocolHeader(headers) {
	if (hasWebSocketProtocolHeader(headers)) throw new RequestError("Do not set `Sec-WebSocket-Protocol` header manually; use the `protocols` option instead.");
}
function normalizeWebSocketProtocolList(protocols) {
	if (protocols === void 0) return;
	return typeof protocols === "string" ? [protocols] : [...protocols];
}
function mergeHeaderTuples(defaults, overrides) {
	if (!defaults) return overrides === void 0 ? void 0 : headersToTuples(overrides);
	if (overrides === void 0) return defaults;
	const overrideTuples = headersToTuples(overrides);
	if (overrideTuples.length === 0) return defaults;
	const overrideKeys = /* @__PURE__ */ new Set();
	for (const tuple of overrideTuples) overrideKeys.add(tuple[0].toLowerCase());
	const merged = [];
	for (const tuple of defaults) if (!overrideKeys.has(tuple[0].toLowerCase())) merged.push(tuple);
	for (const tuple of overrideTuples) merged.push(tuple);
	return merged;
}
function cloneNativeResponse(payload) {
	return {
		status: payload.status,
		headers: payload.headers.map(([name, value]) => [name, value]),
		bodyHandle: payload.bodyHandle,
		bodyBytes: payload.bodyBytes,
		contentLength: payload.contentLength,
		cookies: payload.cookies.map(([name, value]) => [name, value]),
		url: payload.url,
		diagnostics: payload.diagnostics ? { ...payload.diagnostics } : null
	};
}
function releaseNativeBody(handle) {
	if (handle.released) return;
	handle.released = true;
	try {
		nativeBinding.cancelBody(handle.id);
	} catch {}
	bodyHandleFinalizer?.unregister(handle);
}
function markNativeBodyReleased(handle) {
	if (handle.released) return;
	handle.released = true;
	bodyHandleFinalizer?.unregister(handle);
}
function createNativeBodyStream(handle) {
	const stream = new node_stream_web.ReadableStream({
		async pull(controller) {
			try {
				const chunk = await nativeBinding.readBodyChunk(handle.id);
				if (chunk === null) {
					releaseNativeBody(handle);
					controller.close();
					return;
				}
				controller.enqueue(chunk);
			} catch (error) {
				releaseNativeBody(handle);
				controller.error(error);
			}
		},
		cancel() {
			releaseNativeBody(handle);
		}
	});
	bodyHandleFinalizer?.register(stream, handle, handle);
	return stream;
}
function wrapBodyStream(source, onFirstUse) {
	let started = false;
	let reader = null;
	return new node_stream_web.ReadableStream({
		async pull(controller) {
			if (!started) {
				started = true;
				onFirstUse();
			}
			if (!reader) reader = source.getReader();
			try {
				const { done, value } = await reader.read();
				if (done) {
					controller.close();
					return;
				}
				controller.enqueue(value);
			} catch (error) {
				controller.error(error);
			}
		},
		cancel(reason) {
			if (!reader) return source.cancel(reason);
			return reader.cancel(reason);
		}
	});
}
var Response = class Response {
	status;
	ok;
	contentLength;
	url;
	diagnostics;
	type = "basic";
	bodyUsed = false;
	payload;
	requestUrl;
	redirectedMemo;
	headersInit;
	headersInstance;
	cookiesInit;
	cookiesRecord;
	inlineBody;
	bodySource;
	bodyStream;
	nativeHandleAvailable;
	nativeHandle;
	constructor(payload, requestUrl, bodySource) {
		this.payload = payload;
		this.requestUrl = requestUrl;
		this.status = this.payload.status;
		this.ok = this.status >= 200 && this.status < 300;
		this.headersInit = this.payload.headers;
		this.headersInstance = null;
		this.url = this.payload.url;
		this.diagnostics = this.payload.diagnostics ?? null;
		this.cookiesInit = this.payload.cookies;
		this.cookiesRecord = null;
		this.contentLength = this.payload.contentLength ?? null;
		this.inlineBody = this.payload.bodyBytes ?? null;
		this.nativeHandle = null;
		if (typeof bodySource !== "undefined") {
			this.bodySource = bodySource;
			this.nativeHandleAvailable = false;
		} else if (this.inlineBody !== null) {
			this.bodySource = null;
			this.nativeHandleAvailable = false;
		} else if (this.payload.bodyHandle !== null) {
			this.bodySource = null;
			this.nativeHandleAvailable = true;
			this.nativeHandle = {
				id: this.payload.bodyHandle,
				released: false
			};
			bodyHandleFinalizer?.register(this, this.nativeHandle, this.nativeHandle);
		} else {
			this.bodySource = null;
			this.nativeHandleAvailable = false;
		}
		this.bodyStream = void 0;
	}
	get redirected() {
		if (this.redirectedMemo !== void 0) return this.redirectedMemo;
		if (this.url === this.requestUrl) {
			this.redirectedMemo = false;
			return false;
		}
		const normalizedRequestUrl = normalizeUrlForComparison(this.requestUrl);
		this.redirectedMemo = normalizedRequestUrl ? this.url !== normalizedRequestUrl : true;
		return this.redirectedMemo;
	}
	get statusText() {
		return node_http.STATUS_CODES[this.status] ?? "";
	}
	get headers() {
		if (!this.headersInstance) this.headersInstance = new Headers(this.headersInit);
		return this.headersInstance;
	}
	get cookies() {
		if (!this.cookiesRecord) {
			const record = Object.create(null);
			for (const [name, value] of this.cookiesInit) {
				const existing = record[name];
				if (existing === void 0) record[name] = value;
				else if (Array.isArray(existing)) existing.push(value);
				else record[name] = [existing, value];
			}
			this.cookiesRecord = record;
		}
		return this.cookiesRecord;
	}
	get body() {
		if (this.inlineBody && this.bodySource === null) {
			const bytes = this.inlineBody;
			this.inlineBody = null;
			this.bodySource = new node_stream_web.ReadableStream({ start(controller) {
				controller.enqueue(bytes);
				controller.close();
			} });
		}
		if (this.inlineBody === null && this.payload.bodyHandle === null && this.bodySource === null) return null;
		if (this.bodySource === null && this.nativeHandleAvailable && this.payload.bodyHandle !== null) {
			if (this.nativeHandle) bodyHandleFinalizer?.unregister(this.nativeHandle);
			const handle = this.nativeHandle ?? {
				id: this.payload.bodyHandle,
				released: false
			};
			this.nativeHandle = handle;
			this.bodySource = createNativeBodyStream(handle);
			this.nativeHandleAvailable = false;
		}
		if (this.bodySource === null) return null;
		if (this.bodyStream === void 0) this.bodyStream = wrapBodyStream(this.bodySource, () => {
			this.bodyUsed = true;
		});
		return this.bodyStream;
	}
	async json() {
		const text = await this.text();
		return JSON.parse(text);
	}
	async arrayBuffer() {
		const bytes = await this.consumeBody();
		const { buffer, byteOffset, byteLength } = bytes;
		if (buffer instanceof ArrayBuffer) {
			if (byteOffset === 0 && byteLength === buffer.byteLength) return buffer;
			return buffer.slice(byteOffset, byteOffset + byteLength);
		}
		const view = new Uint8Array(byteLength);
		view.set(bytes);
		return view.buffer;
	}
	async bytes() {
		return new Uint8Array(await this.arrayBuffer());
	}
	async text() {
		const bytes = await this.consumeBody();
		return UTF8_DECODER.decode(bytes);
	}
	async blob() {
		const bytes = await this.consumeBody();
		const contentType = this.headers.get("content-type") ?? "";
		return new Blob([bytes], contentType ? { type: contentType } : void 0);
	}
	async formData() {
		const bytes = await this.consumeBody();
		const contentType = this.headers.get("content-type");
		return new globalThis.Response(bytes, contentType ? { headers: { "content-type": contentType } } : void 0).formData();
	}
	readable() {
		this.assertBodyAvailable();
		this.bodyUsed = true;
		const stream = this.body;
		if (stream === null) return node_stream.Readable.from([]);
		return node_stream.Readable.fromWeb(stream);
	}
	clone() {
		if (this.bodyUsed) throw new TypeError("Cannot clone a Response whose body is already used");
		if (this.nativeHandleAvailable && this.payload.bodyHandle !== null) {
			if (this.nativeHandle) bodyHandleFinalizer?.unregister(this.nativeHandle);
			const handle = this.nativeHandle ?? {
				id: this.payload.bodyHandle,
				released: false
			};
			this.nativeHandle = handle;
			this.bodySource = createNativeBodyStream(handle);
			this.nativeHandleAvailable = false;
		}
		if (this.bodySource === null) return new Response(cloneNativeResponse(this.payload), this.requestUrl, null);
		const [branchA, branchB] = this.bodySource.tee();
		this.bodySource = branchA;
		this.bodyStream = void 0;
		return new Response(cloneNativeResponse(this.payload), this.requestUrl, branchB);
	}
	assertBodyAvailable() {
		if (this.bodyUsed) throw new TypeError("Response body is already used");
	}
	async consumeBody() {
		this.assertBodyAvailable();
		this.bodyUsed = true;
		if (this.inlineBody) {
			const bytes = this.inlineBody;
			this.inlineBody = null;
			return bytes;
		}
		if (this.nativeHandleAvailable && this.payload.bodyHandle !== null) {
			this.nativeHandleAvailable = false;
			try {
				return await nativeBinding.readBodyAll(this.payload.bodyHandle);
			} catch (error) {
				if (String(error).includes("Body handle") && String(error).includes("not found")) return Buffer.alloc(0);
				throw error;
			} finally {
				if (this.nativeHandle) markNativeBodyReleased(this.nativeHandle);
			}
		}
		const stream = this.body;
		if (!stream) return Buffer.alloc(0);
		const reader = stream.getReader();
		const chunks = [];
		try {
			while (true) {
				const { done, value } = await reader.read();
				if (done) break;
				if (value && value.byteLength > 0) if (Buffer.isBuffer(value)) chunks.push(value);
				else chunks.push(Buffer.from(value.buffer, value.byteOffset, value.byteLength));
			}
		} finally {}
		return chunks.length === 0 ? Buffer.alloc(0) : Buffer.concat(chunks);
	}
};
var Transport = class {
	id;
	disposed = false;
	constructor(id) {
		this.id = id;
	}
	get closed() {
		return this.disposed;
	}
	async close() {
		if (this.disposed) return;
		this.disposed = true;
		try {
			nativeBinding.dropTransport(this.id);
		} catch (error) {
			throw new RequestError(String(error));
		}
	}
	async [Symbol.asyncDispose]() {
		await this.close();
	}
};
var Session = class {
	id;
	disposed = false;
	defaults;
	constructor(id, defaults) {
		this.id = id;
		this.defaults = defaults;
	}
	get closed() {
		return this.disposed;
	}
	ensureActive() {
		if (this.disposed) throw new RequestError("Session has been closed");
	}
	/** @internal */
	getDefaults() {
		const snapshot = { ...this.defaults };
		if (this.defaults.defaultHeaders) snapshot.defaultHeaders = [...this.defaults.defaultHeaders];
		return snapshot;
	}
	/** @internal */
	_defaultsRef() {
		return this.defaults;
	}
	async fetch(input, init) {
		this.ensureActive();
		return fetch(input, init ? {
			...init,
			session: this
		} : { session: this });
	}
	async clearCookies() {
		this.ensureActive();
		try {
			nativeBinding.clearSession(this.id);
		} catch (error) {
			throw new RequestError(String(error));
		}
	}
	getCookies(url) {
		this.ensureActive();
		try {
			return nativeBinding.getCookies(this.id, String(url));
		} catch (error) {
			throw new RequestError(String(error));
		}
	}
	getAllCookies() {
		this.ensureActive();
		try {
			return nativeBinding.getAllCookies(this.id);
		} catch (error) {
			throw new RequestError(String(error));
		}
	}
	setCookie(name, value, url) {
		this.ensureActive();
		try {
			nativeBinding.setCookie(this.id, name, value, String(url));
		} catch (error) {
			throw new RequestError(String(error));
		}
	}
	async websocket(urlOrOptions, options) {
		this.ensureActive();
		const normalized = normalizeSessionWebSocketArgs(urlOrOptions, options);
		validateWebSocketProtocols(normalized.options.protocols);
		assertNoManualWebSocketProtocolHeader(normalized.options.headers);
		const protocols = normalizeWebSocketProtocolList(normalized.options.protocols);
		const transportId = this.defaults.transportId;
		if (!transportId) throw new RequestError("Session has no transport. Create the session with browser/os options or pass a transport to use session.websocket().");
		return WebSocket._connectWithInit({
			_internal: true,
			url: normalized.url,
			options: normalized.options,
			openDispatchMode: "deferred",
			connect: (callbacks) => nativeBinding.websocketConnectSession({
				url: normalized.url,
				sessionId: this.id,
				transportId,
				headers: headersToTuples(normalized.options.headers ?? {}),
				...protocols && protocols.length > 0 && { protocols },
				...normalized.options.maxFrameSize !== void 0 && { maxFrameSize: normalized.options.maxFrameSize },
				...normalized.options.maxMessageSize !== void 0 && { maxMessageSize: normalized.options.maxMessageSize },
				onMessage: callbacks.onMessage,
				onClose: callbacks.onClose,
				onError: callbacks.onError
			}),
			legacyCallbacks: normalized.legacyCallbacks
		});
	}
	async close() {
		if (this.disposed) return;
		this.disposed = true;
		const transportId = this.defaults.transportId;
		const ownsTransport = this.defaults.ownsTransport;
		try {
			nativeBinding.dropSession(this.id);
		} catch (error) {
			if (!ownsTransport || !transportId) throw new RequestError(String(error));
			const originalError = error;
			try {
				nativeBinding.dropTransport(transportId);
			} catch {}
			throw new RequestError(String(originalError));
		}
		if (ownsTransport && transportId) try {
			nativeBinding.dropTransport(transportId);
		} catch (error) {
			throw new RequestError(String(error));
		}
	}
	async [Symbol.asyncDispose]() {
		await this.close();
	}
};
function resolveSessionContext(config) {
	const requestedMode = config.cookieMode ?? "ephemeral";
	const sessionCandidate = config.session;
	const providedSessionId = typeof config.sessionId === "string" ? config.sessionId.trim() : void 0;
	if (sessionCandidate && providedSessionId) throw new RequestError("Provide either `session` or `sessionId`, not both.");
	if (sessionCandidate) {
		if (!(sessionCandidate instanceof Session)) throw new RequestError("`session` must be created via createSession()");
		if (sessionCandidate.closed) throw new RequestError("Session has been closed");
		return {
			sessionId: sessionCandidate.id,
			cookieMode: "session",
			dropAfterRequest: false,
			defaults: sessionCandidate._defaultsRef()
		};
	}
	if (providedSessionId) {
		if (!providedSessionId) throw new RequestError("sessionId must not be empty");
		if (requestedMode === "ephemeral") throw new RequestError("cookieMode 'ephemeral' cannot be combined with sessionId");
		return {
			sessionId: providedSessionId,
			cookieMode: "session",
			dropAfterRequest: false
		};
	}
	if (requestedMode === "session") throw new RequestError("cookieMode 'session' requires a session or sessionId");
	return {
		sessionId: generateEphemeralSessionId(),
		cookieMode: "ephemeral",
		dropAfterRequest: true
	};
}
function resolveTransportContext(config, sessionDefaults) {
	if (config.transport !== void 0) {
		if (!(config.transport instanceof Transport)) throw new RequestError("`transport` must be created via createTransport()");
		if (config.transport.closed) throw new RequestError("Transport has been closed");
		const hasProxy = config.proxy !== void 0;
		if (config.browser !== void 0 || config.os !== void 0 || config.emulation !== void 0 || hasProxy || config.insecure !== void 0 || config.trustStore !== void 0) throw new RequestError("`transport` cannot be combined with browser/os/emulation/proxy/insecure/trustStore options");
		return {
			transportId: config.transport.id,
			...config.captureDiagnostics !== void 0 && { captureDiagnostics: config.captureDiagnostics }
		};
	}
	if (sessionDefaults?.transportId) {
		if (config.emulation !== void 0) throw new RequestError("Session emulation cannot be changed after creation");
		if (config.browser !== void 0) {
			if (resolveProfile(config.browser) !== (sessionDefaults.transportMode.kind === "custom" ? void 0 : sessionDefaults.transportMode.browser)) throw new RequestError("Session browser cannot be changed after creation");
		}
		if (config.os !== void 0) {
			validateOperatingSystem(config.os);
			const lockedOs = sessionDefaults.transportMode.kind === "custom" ? void 0 : sessionDefaults.transportMode.os;
			if (config.os !== lockedOs) throw new RequestError("Session operating system cannot be changed after creation");
		}
		const initHasProxy = Object.hasOwn(config, "proxy");
		const requestedProxy = initHasProxy ? config.proxy : void 0;
		if (initHasProxy && requestedProxy !== void 0 && (sessionDefaults.proxy ?? null) !== (requestedProxy ?? null)) throw new RequestError("Session proxy cannot be changed after creation");
		if (config.insecure !== void 0) {
			const lockedInsecure = sessionDefaults.insecure ?? false;
			if (config.insecure !== lockedInsecure) throw new RequestError("Session insecure setting cannot be changed after creation");
		}
		if (config.trustStore !== void 0) {
			const lockedTrustStore = sessionDefaults.trustStore ?? DEFAULT_TRUST_STORE;
			if (config.trustStore !== lockedTrustStore) throw new RequestError("Session trustStore setting cannot be changed after creation");
		}
		const captureDiagnostics = config.captureDiagnostics ?? sessionDefaults.captureDiagnostics;
		return {
			transportId: sessionDefaults.transportId,
			...captureDiagnostics !== void 0 && { captureDiagnostics }
		};
	}
	const resolved = { mode: resolveEmulationMode(config.browser, config.os, config.emulation) };
	if (config.proxy !== void 0) resolved.proxy = config.proxy;
	if (config.insecure !== void 0) resolved.insecure = config.insecure;
	resolved.trustStore = config.trustStore ?? DEFAULT_TRUST_STORE;
	if (config.captureDiagnostics !== void 0) resolved.captureDiagnostics = config.captureDiagnostics;
	return resolved;
}
function createAbortError(reason) {
	const fallbackMessage = typeof reason === "string" ? reason : "The operation was aborted";
	if (typeof DOMException !== "undefined" && reason instanceof DOMException) return reason.name === "AbortError" ? reason : new DOMException(reason.message || fallbackMessage, "AbortError");
	if (reason instanceof Error) {
		const error = new Error(reason.message);
		error.name = "AbortError";
		error.cause = reason;
		return error;
	}
	if (typeof DOMException !== "undefined") return new DOMException(fallbackMessage, "AbortError");
	const error = new Error(fallbackMessage);
	error.name = "AbortError";
	return error;
}
function isAbortError(error) {
	return Boolean(error) && typeof error.name === "string" && error.name === "AbortError";
}
const REQUEST_ID_MAX = 2 ** 48;
let requestIdCounter = Math.trunc(Number(process.hrtime.bigint() % BigInt(REQUEST_ID_MAX - 1))) + 1;
function generateRequestId() {
	requestIdCounter += 1;
	if (requestIdCounter >= REQUEST_ID_MAX) requestIdCounter = 1;
	return requestIdCounter;
}
function setupAbort(signal, cancelNative) {
	if (!signal) return null;
	if (signal.aborted) {
		cancelNative();
		throw createAbortError(signal.reason);
	}
	let onAbortListener;
	const promise = new Promise((_, reject) => {
		onAbortListener = () => {
			cancelNative();
			reject(createAbortError(signal.reason));
		};
		signal.addEventListener("abort", onAbortListener, { once: true });
	});
	const cleanup = () => {
		if (onAbortListener) {
			signal.removeEventListener("abort", onAbortListener);
			onAbortListener = void 0;
		}
	};
	return {
		promise,
		cleanup
	};
}
function coerceUrlInput(input) {
	if (input instanceof URL) return input.href;
	if (input.length === 0) throw new RequestError("URL is required");
	if (input.charCodeAt(0) > 32 && input.charCodeAt(input.length - 1) > 32) return input;
	const trimmed = input.trim();
	if (trimmed.length === 0) throw new RequestError("URL is required");
	return trimmed;
}
function isRequestLike(input) {
	if (!input || typeof input !== "object") return false;
	if (typeof Request !== "undefined" && input instanceof Request) return true;
	const candidate = input;
	return typeof candidate.url === "string" && typeof candidate.method === "string" && typeof candidate.arrayBuffer === "function" && typeof candidate.redirect === "string";
}
async function resolveFetchArgs(input, init) {
	if (!isRequestLike(input)) return {
		url: coerceUrlInput(input),
		init: init ?? {}
	};
	const mergedInit = init ? { ...init } : {};
	if (mergedInit.method === void 0) mergedInit.method = input.method;
	if (mergedInit.headers === void 0) mergedInit.headers = input.headers;
	if (mergedInit.redirect === void 0 && (input.redirect === "follow" || input.redirect === "manual" || input.redirect === "error")) mergedInit.redirect = input.redirect;
	if (mergedInit.signal === void 0) mergedInit.signal = input.signal;
	if (mergedInit.body === void 0 && input.body !== null) {
		if (input.bodyUsed) throw new TypeError("Request body is already used");
		mergedInit.body = Buffer.from(await input.arrayBuffer());
	}
	return {
		url: coerceUrlInput(input.url),
		init: mergedInit
	};
}
function normalizeUrlForComparison(value) {
	try {
		return new URL(value).toString();
	} catch {
		return null;
	}
}
function validateRedirectMode(mode) {
	if (mode === void 0 || mode === "follow" || mode === "manual" || mode === "error") return;
	throw new RequestError(`Redirect mode '${mode}' is not supported`);
}
async function serializeBody(body) {
	if (body === null || body === void 0) return {};
	if (typeof body === "string") return { body: Buffer.from(body, "utf8") };
	if (Buffer.isBuffer(body)) return { body };
	if (body instanceof URLSearchParams) return {
		body: Buffer.from(body.toString(), "utf8"),
		contentType: "application/x-www-form-urlencoded;charset=UTF-8"
	};
	if (body instanceof ArrayBuffer) return { body: Buffer.from(body) };
	if (ArrayBuffer.isView(body)) return { body: Buffer.from(body.buffer, body.byteOffset, body.byteLength) };
	if (typeof Blob !== "undefined" && body instanceof Blob) return {
		body: Buffer.from(await body.arrayBuffer()),
		...body.type ? { contentType: body.type } : {}
	};
	if (typeof FormData !== "undefined" && body instanceof FormData) {
		const encoded = new globalThis.Response(body);
		const contentType = encoded.headers.get("content-type") ?? void 0;
		return {
			body: Buffer.from(await encoded.arrayBuffer()),
			...contentType ? { contentType } : {}
		};
	}
	if (typeof body.getReader === "function") {
		const reader = body.getReader();
		const chunks = [];
		for (;;) {
			const { done, value } = await reader.read();
			if (done) break;
			chunks.push(value);
		}
		return { body: Buffer.concat(chunks) };
	}
	if (typeof body[Symbol.asyncIterator] === "function" || typeof body[Symbol.iterator] === "function") {
		const chunks = [];
		for await (const chunk of body) {
			if (!ArrayBuffer.isView(chunk)) throw new TypeError("Iterable request bodies must yield Uint8Array chunks");
			chunks.push(chunk);
		}
		return { body: Buffer.concat(chunks) };
	}
	throw new TypeError("Unsupported body type; expected string, Buffer, ArrayBuffer, ArrayBufferView, URLSearchParams, Blob, FormData, ReadableStream, or an iterable of Uint8Array");
}
function ensureMethod(method) {
	if (method === void 0 || method.length === 0) return "GET";
	switch (method) {
		case "GET":
		case "POST":
		case "PUT":
		case "DELETE":
		case "PATCH":
		case "HEAD":
		case "OPTIONS": return method;
	}
	const normalized = method.trim().toUpperCase();
	return normalized.length > 0 ? normalized : "GET";
}
function ensureBodyAllowed(method, body) {
	if (body === void 0) return;
	if (method === "GET" || method === "HEAD") throw new RequestError(`Request with ${method} method cannot have a body`);
}
function validateBrowserProfile(browser) {
	if (browser === void 0) return;
	if (typeof browser !== "string" || browser.trim().length === 0) throw new RequestError("Browser profile must not be empty");
	if (!getProfileSet().has(browser) && !Object.hasOwn(BROWSER_ALIASES, browser)) throw new RequestError(`Invalid browser profile: ${browser}. Available aliases: ${Object.keys(BROWSER_ALIASES).join(", ")}. Available profiles: ${getProfiles().join(", ")}`);
}
function validateOperatingSystem(os) {
	if (os === void 0) return;
	if (typeof os !== "string" || os.trim().length === 0) throw new RequestError("Operating system must not be empty");
	if (!getOperatingSystemSet().has(os)) throw new RequestError(`Invalid operating system: ${os}. Available options: ${getOperatingSystems().join(", ")}`);
}
function validateTimeout(timeout) {
	if (timeout === void 0) return;
	if (typeof timeout !== "number" || !Number.isFinite(timeout)) throw new RequestError("Timeout must be a finite number");
	if (timeout < 0) throw new RequestError("Timeout must be 0 (no timeout) or a positive number");
}
function validateTrustStore(trustStore) {
	if (trustStore === void 0) return;
	if (trustStore !== "combined" && trustStore !== "mozilla" && trustStore !== "defaultPaths") throw new RequestError("trustStore must be one of: combined, mozilla, defaultPaths");
}
function validatePositiveNumber(value, label) {
	if (typeof value !== "number" || !Number.isFinite(value)) throw new RequestError(`${label} must be a finite number`);
	if (value <= 0) throw new RequestError(`${label} must be greater than 0`);
}
function validateNonNegativeInteger(value, label) {
	if (typeof value !== "number" || !Number.isFinite(value) || !Number.isInteger(value)) throw new RequestError(`${label} must be an integer`);
	if (value < 0) throw new RequestError(`${label} must be greater than or equal to 0`);
}
function validatePositiveInteger(value, label) {
	if (typeof value !== "number" || !Number.isFinite(value) || !Number.isInteger(value)) throw new RequestError(`${label} must be an integer`);
	if (value <= 0) throw new RequestError(`${label} must be greater than 0`);
}
function validateResolve(resolve) {
	if (typeof resolve !== "object" || resolve === null || Array.isArray(resolve)) throw new RequestError("resolve must be an object mapping hosts to addresses");
	for (const [host, value] of Object.entries(resolve)) {
		const addresses = Array.isArray(value) ? value : [value];
		if (addresses.length === 0) throw new RequestError(`resolve['${host}'] must provide at least one address`);
		for (const address of addresses) if (typeof address !== "string" || address.trim() === "") throw new RequestError(`resolve['${host}'] addresses must be non-empty strings`);
	}
}
function validateIntegerInRange(value, min, max, label) {
	validateNonNegativeInteger(value, label);
	if (value < min || value > max) throw new RequestError(`${label} must be between ${min} and ${max}`);
}
const SUPPORTED_ALPN_PROTOCOLS = /* @__PURE__ */ new Set([
	"HTTP1",
	"HTTP2",
	"HTTP3"
]);
const SUPPORTED_ALPS_PROTOCOLS = /* @__PURE__ */ new Set([
	"HTTP1",
	"HTTP2",
	"HTTP3"
]);
const SUPPORTED_CERTIFICATE_COMPRESSION_ALGORITHMS = /* @__PURE__ */ new Set([
	"zlib",
	"brotli",
	"zstd"
]);
const HTTP2_SETTING_IDS = /* @__PURE__ */ new Set([
	"HeaderTableSize",
	"EnablePush",
	"MaxConcurrentStreams",
	"InitialWindowSize",
	"MaxFrameSize",
	"MaxHeaderListSize",
	"EnableConnectProtocol",
	"NoRfc7540Priorities"
]);
const HTTP2_PSEUDO_HEADER_IDS = /* @__PURE__ */ new Set([
	"Method",
	"Scheme",
	"Authority",
	"Path",
	"Protocol"
]);
const TLS_VERSION_ALIASES = /* @__PURE__ */ new Map([
	["1.0", "1.0"],
	["1.1", "1.1"],
	["1.2", "1.2"],
	["1.3", "1.3"],
	["tls1.0", "1.0"],
	["tls1.1", "1.1"],
	["tls1.2", "1.2"],
	["tls1.3", "1.3"]
]);
function isNonEmpty(value) {
	for (const _ in value) return true;
	return false;
}
function normalizeProtocolList(value, label, allowed) {
	if (value === void 0) return;
	if (!Array.isArray(value)) throw new RequestError(`${label} must be an array`);
	for (const protocol of value) if (!allowed.has(protocol)) throw new RequestError(`${label} values must be one of: HTTP1, HTTP2, HTTP3`);
	return [...value];
}
function normalizeTlsVersion(value, label) {
	if (value === void 0) return;
	if (typeof value !== "string") throw new RequestError(`${label} must be a string`);
	const normalized = TLS_VERSION_ALIASES.get(value.trim().toLowerCase());
	if (!normalized) throw new RequestError(`${label} must be one of: 1.0, 1.1, 1.2, 1.3`);
	return normalized;
}
function normalizeOrigHeaders(origHeaders) {
	if (origHeaders === void 0) return;
	if (!Array.isArray(origHeaders)) throw new RequestError("emulation.origHeaders must be an array of strings");
	const normalized = [];
	const seen = /* @__PURE__ */ new Set();
	for (const entry of origHeaders) {
		if (typeof entry !== "string" || entry.trim().length === 0) throw new RequestError("emulation.origHeaders entries must be non-empty strings");
		const trimmed = entry.trim();
		const duplicateKey = trimmed.toLowerCase();
		if (seen.has(duplicateKey)) throw new RequestError(`Duplicate emulation.origHeaders entry: ${trimmed}`);
		seen.add(duplicateKey);
		normalized.push(trimmed);
	}
	return normalized.length > 0 ? normalized : void 0;
}
function normalizeCustomTlsOptions(options) {
	if (options === void 0) return;
	const normalized = {};
	const alpnProtocols = normalizeProtocolList(options.alpnProtocols, "emulation.tlsOptions.alpnProtocols", SUPPORTED_ALPN_PROTOCOLS);
	if (alpnProtocols !== void 0) normalized.alpnProtocols = alpnProtocols;
	const alpsProtocols = normalizeProtocolList(options.alpsProtocols, "emulation.tlsOptions.alpsProtocols", SUPPORTED_ALPS_PROTOCOLS);
	if (alpsProtocols !== void 0) normalized.alpsProtocols = alpsProtocols;
	const minTlsVersion = normalizeTlsVersion(options.minTlsVersion, "emulation.tlsOptions.minTlsVersion");
	if (minTlsVersion !== void 0) normalized.minTlsVersion = minTlsVersion;
	const maxTlsVersion = normalizeTlsVersion(options.maxTlsVersion, "emulation.tlsOptions.maxTlsVersion");
	if (maxTlsVersion !== void 0) normalized.maxTlsVersion = maxTlsVersion;
	if (options.alpsUseNewCodepoint !== void 0) normalized.alpsUseNewCodepoint = options.alpsUseNewCodepoint;
	if (options.sessionTicket !== void 0) normalized.sessionTicket = options.sessionTicket;
	if (options.preSharedKey !== void 0) normalized.preSharedKey = options.preSharedKey;
	if (options.enableEchGrease !== void 0) normalized.enableEchGrease = options.enableEchGrease;
	if (options.permuteExtensions !== void 0) normalized.permuteExtensions = options.permuteExtensions;
	if (options.greaseEnabled !== void 0) normalized.greaseEnabled = options.greaseEnabled;
	if (options.enableOcspStapling !== void 0) normalized.enableOcspStapling = options.enableOcspStapling;
	if (options.enableSignedCertTimestamps !== void 0) normalized.enableSignedCertTimestamps = options.enableSignedCertTimestamps;
	if (options.pskSkipSessionTicket !== void 0) normalized.pskSkipSessionTicket = options.pskSkipSessionTicket;
	if (options.pskDheKe !== void 0) normalized.pskDheKe = options.pskDheKe;
	if (options.renegotiation !== void 0) normalized.renegotiation = options.renegotiation;
	if (options.aesHwOverride !== void 0) normalized.aesHwOverride = options.aesHwOverride;
	if (options.preserveTls13CipherList !== void 0) normalized.preserveTls13CipherList = options.preserveTls13CipherList;
	if (options.randomAesHwOverride !== void 0) normalized.randomAesHwOverride = options.randomAesHwOverride;
	if (options.delegatedCredentials !== void 0) normalized.delegatedCredentials = options.delegatedCredentials;
	if (options.curvesList !== void 0) normalized.curvesList = options.curvesList;
	if (options.cipherList !== void 0) normalized.cipherList = options.cipherList;
	if (options.sigalgsList !== void 0) normalized.sigalgsList = options.sigalgsList;
	if (options.recordSizeLimit !== void 0) {
		validateIntegerInRange(options.recordSizeLimit, 0, 65535, "emulation.tlsOptions.recordSizeLimit");
		normalized.recordSizeLimit = options.recordSizeLimit;
	}
	if (options.certificateCompressionAlgorithms !== void 0) {
		if (!Array.isArray(options.certificateCompressionAlgorithms)) throw new RequestError("emulation.tlsOptions.certificateCompressionAlgorithms must be an array");
		const algorithms = [];
		const seen = /* @__PURE__ */ new Set();
		for (const algorithm of options.certificateCompressionAlgorithms) {
			if (!SUPPORTED_CERTIFICATE_COMPRESSION_ALGORITHMS.has(algorithm)) throw new RequestError("emulation.tlsOptions.certificateCompressionAlgorithms values must be one of: zlib, brotli, zstd");
			if (seen.has(algorithm)) throw new RequestError(`Duplicate emulation.tlsOptions.certificateCompressionAlgorithms entry: ${algorithm}`);
			seen.add(algorithm);
			algorithms.push(algorithm);
		}
		normalized.certificateCompressionAlgorithms = algorithms;
	}
	if (options.extensionPermutation !== void 0) {
		if (!Array.isArray(options.extensionPermutation)) throw new RequestError("emulation.tlsOptions.extensionPermutation must be an array");
		const permutation = [];
		const seen = /* @__PURE__ */ new Set();
		for (const extensionId of options.extensionPermutation) {
			validateIntegerInRange(extensionId, 0, 65535, "emulation.tlsOptions.extensionPermutation");
			if (seen.has(extensionId)) throw new RequestError(`Duplicate emulation.tlsOptions.extensionPermutation entry: ${extensionId}`);
			seen.add(extensionId);
			permutation.push(extensionId);
		}
		normalized.extensionPermutation = permutation;
	}
	return isNonEmpty(normalized) ? normalized : void 0;
}
function normalizeCustomHttp1Options(options) {
	if (options === void 0) return;
	const normalized = {};
	if (options.http09Responses !== void 0) normalized.http09Responses = options.http09Responses;
	if (options.writev !== void 0) normalized.writev = options.writev;
	if (options.ignoreInvalidHeadersInResponses !== void 0) normalized.ignoreInvalidHeadersInResponses = options.ignoreInvalidHeadersInResponses;
	if (options.allowSpacesAfterHeaderNameInResponses !== void 0) normalized.allowSpacesAfterHeaderNameInResponses = options.allowSpacesAfterHeaderNameInResponses;
	if (options.allowObsoleteMultilineHeadersInResponses !== void 0) normalized.allowObsoleteMultilineHeadersInResponses = options.allowObsoleteMultilineHeadersInResponses;
	if (options.maxHeaders !== void 0) {
		validateNonNegativeInteger(options.maxHeaders, "emulation.http1Options.maxHeaders");
		normalized.maxHeaders = options.maxHeaders;
	}
	if (options.readBufExactSize !== void 0) {
		validateNonNegativeInteger(options.readBufExactSize, "emulation.http1Options.readBufExactSize");
		normalized.readBufExactSize = options.readBufExactSize;
	}
	if (options.maxBufSize !== void 0) {
		validateNonNegativeInteger(options.maxBufSize, "emulation.http1Options.maxBufSize");
		if (options.maxBufSize < 8192) throw new RequestError("emulation.http1Options.maxBufSize must be greater than or equal to 8192");
		normalized.maxBufSize = options.maxBufSize;
	}
	if (normalized.readBufExactSize !== void 0 && normalized.maxBufSize !== void 0) throw new RequestError("emulation.http1Options.readBufExactSize and maxBufSize cannot both be set");
	return isNonEmpty(normalized) ? normalized : void 0;
}
function normalizeHttp2StreamDependency(dependency, label) {
	if (!isPlainObject(dependency)) throw new RequestError(`${label} must be an object`);
	validateIntegerInRange(dependency.dependencyId, 0, 2147483647, `${label}.dependencyId`);
	validateIntegerInRange(dependency.weight, 0, 255, `${label}.weight`);
	const normalized = {
		dependencyId: dependency.dependencyId,
		weight: dependency.weight
	};
	if (dependency.exclusive !== void 0) normalized.exclusive = dependency.exclusive;
	return normalized;
}
function normalizeCustomHttp2Options(options) {
	if (options === void 0) return;
	const normalized = {};
	if (options.adaptiveWindow !== void 0) normalized.adaptiveWindow = options.adaptiveWindow;
	if (options.keepAliveWhileIdle !== void 0) normalized.keepAliveWhileIdle = options.keepAliveWhileIdle;
	if (options.enablePush !== void 0) normalized.enablePush = options.enablePush;
	if (options.enableConnectProtocol !== void 0) normalized.enableConnectProtocol = options.enableConnectProtocol;
	if (options.noRfc7540Priorities !== void 0) normalized.noRfc7540Priorities = options.noRfc7540Priorities;
	if (options.initialStreamId !== void 0) {
		validateNonNegativeInteger(options.initialStreamId, "emulation.http2Options.initialStreamId");
		normalized.initialStreamId = options.initialStreamId;
	}
	if (options.initialConnectionWindowSize !== void 0) {
		validateNonNegativeInteger(options.initialConnectionWindowSize, "emulation.http2Options.initialConnectionWindowSize");
		normalized.initialConnectionWindowSize = options.initialConnectionWindowSize;
	}
	if (options.initialWindowSize !== void 0) {
		validateNonNegativeInteger(options.initialWindowSize, "emulation.http2Options.initialWindowSize");
		normalized.initialWindowSize = options.initialWindowSize;
	}
	if (options.initialMaxSendStreams !== void 0) {
		validateNonNegativeInteger(options.initialMaxSendStreams, "emulation.http2Options.initialMaxSendStreams");
		normalized.initialMaxSendStreams = options.initialMaxSendStreams;
	}
	if (options.maxFrameSize !== void 0) {
		validateNonNegativeInteger(options.maxFrameSize, "emulation.http2Options.maxFrameSize");
		normalized.maxFrameSize = options.maxFrameSize;
	}
	if (options.keepAliveInterval !== void 0) {
		validateNonNegativeInteger(options.keepAliveInterval, "emulation.http2Options.keepAliveInterval");
		normalized.keepAliveInterval = options.keepAliveInterval;
	}
	if (options.keepAliveTimeout !== void 0) {
		validateNonNegativeInteger(options.keepAliveTimeout, "emulation.http2Options.keepAliveTimeout");
		normalized.keepAliveTimeout = options.keepAliveTimeout;
	}
	if (options.maxConcurrentResetStreams !== void 0) {
		validateNonNegativeInteger(options.maxConcurrentResetStreams, "emulation.http2Options.maxConcurrentResetStreams");
		normalized.maxConcurrentResetStreams = options.maxConcurrentResetStreams;
	}
	if (options.maxSendBufferSize !== void 0) {
		validateNonNegativeInteger(options.maxSendBufferSize, "emulation.http2Options.maxSendBufferSize");
		normalized.maxSendBufferSize = options.maxSendBufferSize;
	}
	if (options.maxConcurrentStreams !== void 0) {
		validateNonNegativeInteger(options.maxConcurrentStreams, "emulation.http2Options.maxConcurrentStreams");
		normalized.maxConcurrentStreams = options.maxConcurrentStreams;
	}
	if (options.maxHeaderListSize !== void 0) {
		validateNonNegativeInteger(options.maxHeaderListSize, "emulation.http2Options.maxHeaderListSize");
		normalized.maxHeaderListSize = options.maxHeaderListSize;
	}
	if (options.maxPendingAcceptResetStreams !== void 0) {
		validateNonNegativeInteger(options.maxPendingAcceptResetStreams, "emulation.http2Options.maxPendingAcceptResetStreams");
		normalized.maxPendingAcceptResetStreams = options.maxPendingAcceptResetStreams;
	}
	if (options.headerTableSize !== void 0) {
		validateNonNegativeInteger(options.headerTableSize, "emulation.http2Options.headerTableSize");
		normalized.headerTableSize = options.headerTableSize;
	}
	if (options.settingsOrder !== void 0) {
		if (!Array.isArray(options.settingsOrder)) throw new RequestError("emulation.http2Options.settingsOrder must be an array");
		const settingsOrder = [];
		const seen = /* @__PURE__ */ new Set();
		for (const settingId of options.settingsOrder) {
			if (!HTTP2_SETTING_IDS.has(settingId)) throw new RequestError("emulation.http2Options.settingsOrder contains an unsupported setting id");
			if (seen.has(settingId)) throw new RequestError(`Duplicate emulation.http2Options.settingsOrder entry: ${settingId}`);
			seen.add(settingId);
			settingsOrder.push(settingId);
		}
		normalized.settingsOrder = settingsOrder;
	}
	if (options.headersPseudoOrder !== void 0) {
		if (!Array.isArray(options.headersPseudoOrder)) throw new RequestError("emulation.http2Options.headersPseudoOrder must be an array");
		const headersPseudoOrder = [];
		const seenPseudo = /* @__PURE__ */ new Set();
		for (const pseudoId of options.headersPseudoOrder) {
			if (!HTTP2_PSEUDO_HEADER_IDS.has(pseudoId)) throw new RequestError("emulation.http2Options.headersPseudoOrder contains an unsupported pseudo-header id");
			if (seenPseudo.has(pseudoId)) throw new RequestError(`Duplicate emulation.http2Options.headersPseudoOrder entry: ${pseudoId}`);
			seenPseudo.add(pseudoId);
			headersPseudoOrder.push(pseudoId);
		}
		normalized.headersPseudoOrder = headersPseudoOrder;
	}
	if (options.headersStreamDependency !== void 0) normalized.headersStreamDependency = normalizeHttp2StreamDependency(options.headersStreamDependency, "emulation.http2Options.headersStreamDependency");
	if (options.priorities !== void 0) {
		if (!Array.isArray(options.priorities)) throw new RequestError("emulation.http2Options.priorities must be an array");
		const priorities = [];
		const seenStreamIds = /* @__PURE__ */ new Set();
		for (const [index, priority] of options.priorities.entries()) {
			if (!isPlainObject(priority)) throw new RequestError(`emulation.http2Options.priorities[${index}] must be an object`);
			validatePositiveInteger(priority.streamId, `emulation.http2Options.priorities[${index}].streamId`);
			if (seenStreamIds.has(priority.streamId)) throw new RequestError(`Duplicate emulation.http2Options.priorities streamId: ${priority.streamId}`);
			seenStreamIds.add(priority.streamId);
			priorities.push({
				streamId: priority.streamId,
				dependency: normalizeHttp2StreamDependency(priority.dependency, `emulation.http2Options.priorities[${index}].dependency`)
			});
		}
		normalized.priorities = priorities;
	}
	return isNonEmpty(normalized) ? normalized : void 0;
}
function normalizeCustomEmulationOptions(emulation, allowEmpty) {
	if (emulation === void 0) return;
	if (!isPlainObject(emulation)) throw new RequestError("emulation must be an object");
	const source = emulation;
	const normalized = {};
	const tlsOptions = normalizeCustomTlsOptions(source.tlsOptions);
	if (tlsOptions !== void 0) normalized.tlsOptions = tlsOptions;
	const http1Options = normalizeCustomHttp1Options(source.http1Options);
	if (http1Options !== void 0) normalized.http1Options = http1Options;
	const http2Options = normalizeCustomHttp2Options(source.http2Options);
	if (http2Options !== void 0) normalized.http2Options = http2Options;
	if (source.headers !== void 0) {
		const headers = headersToTuples(source.headers);
		if (headers.length > 0) normalized.headers = headers;
	}
	const origHeaders = normalizeOrigHeaders(source.origHeaders);
	if (origHeaders !== void 0) normalized.origHeaders = origHeaders;
	if (!allowEmpty && !isNonEmpty(normalized)) throw new RequestError("Standalone custom emulation requires at least one of tlsOptions, http1Options, http2Options, headers, or origHeaders");
	return isNonEmpty(normalized) ? normalized : void 0;
}
function serializeCustomEmulationOptions(emulation, allowEmpty) {
	const normalized = normalizeCustomEmulationOptions(emulation, allowEmpty);
	return normalized ? JSON.stringify(normalized) : void 0;
}
function resolveEmulationMode(browser, os, emulation) {
	if (browser !== void 0) {
		const resolvedBrowser = resolveProfile(browser);
		if (os !== void 0) validateOperatingSystem(os);
		const emulationJson = serializeCustomEmulationOptions(emulation, true);
		return {
			kind: "preset",
			browser: resolvedBrowser,
			os: os ?? DEFAULT_OS,
			...emulationJson !== void 0 && { emulationJson }
		};
	}
	if (os !== void 0) {
		validateOperatingSystem(os);
		const emulationJson = serializeCustomEmulationOptions(emulation, true);
		return {
			kind: "preset",
			browser: DEFAULT_BROWSER,
			os,
			...emulationJson !== void 0 && { emulationJson }
		};
	}
	if (emulation !== void 0) {
		const emulationJson = serializeCustomEmulationOptions(emulation, false);
		if (emulationJson === void 0) throw new RequestError("Standalone custom emulation requires at least one of tlsOptions, http1Options, http2Options, headers, or origHeaders");
		return {
			kind: "custom",
			emulationJson
		};
	}
	return {
		kind: "preset",
		browser: DEFAULT_BROWSER,
		os: DEFAULT_OS
	};
}
function applyNativeEmulationMode(target, mode) {
	if (mode.kind === "custom") {
		target.emulationJson = mode.emulationJson;
		return;
	}
	target.browser = mode.browser;
	target.os = mode.os;
	if (mode.emulationJson !== void 0) target.emulationJson = mode.emulationJson;
}
async function dispatchRequest(options, requestUrl, signal) {
	if (!signal) {
		const requestId = generateRequestId();
		let payload;
		try {
			payload = await nativeBinding.request(options, requestId, false);
		} catch (error) {
			if (error instanceof RequestError) throw error;
			throw new RequestError(String(error));
		}
		return new Response(payload, requestUrl);
	}
	const requestId = generateRequestId();
	const cancelNative = () => {
		try {
			nativeBinding.cancelRequest(requestId);
		} catch {}
	};
	const abortHandler = setupAbort(signal, cancelNative);
	const pending = Promise.race([nativeBinding.request(options, requestId, true), abortHandler.promise]);
	let payload;
	try {
		payload = await pending;
	} catch (error) {
		if (isAbortError(error)) throw error;
		if (error instanceof RequestError) throw error;
		throw new RequestError(String(error));
	} finally {
		abortHandler.cleanup();
	}
	return new Response(payload, requestUrl);
}
/**
* Fetch-compatible entry point that adds browser impersonation controls.
*
* **Important:** The default fetch path is isolated and non-persistent by design.
* Each call uses an isolated request context, so cookies are not shared across calls.
* Connection and TLS reuse behavior is handled by the native layer.
*
* **Use {@link createSession} or {@link withSession} if you need:**
* - Cookie persistence across requests
* - Shared session defaults across requests
* - A single session context for multi-step flows
*
* **Concurrency:** The core is unthrottled by design. Callers are expected to implement
* their own concurrency control (e.g., p-limit) if needed. Built-in throttling would
* reduce performance for high-throughput workloads.
*
* @param input - Request URL (string or URL) or a Request object
* @param init - Fetch-compatible init options
*
* @example
* ```typescript
* // Isolated request (no state persistence)
* const response = await fetch('https://example.com');
*
* // For persistent cookies and connection reuse, use a session:
* await withSession(async (session) => {
*   await session.fetch('https://example.com/login', { method: 'POST', body: loginData });
*   await session.fetch('https://example.com/protected'); // Cookies from login are sent
* });
* ```
*/
async function fetch(input, init) {
	const resolved = await resolveFetchArgs(input, init);
	const url = resolved.url;
	const config = resolved.init;
	const sessionContext = resolveSessionContext(config);
	const sessionDefaults = sessionContext.defaults;
	validateRedirectMode(config.redirect);
	if (config.timeout !== void 0) validateTimeout(config.timeout);
	validateTrustStore(config.trustStore);
	const method = ensureMethod(config.method);
	const serializedBody = await serializeBody(config.body ?? null);
	const body = serializedBody.body;
	ensureBodyAllowed(method, body);
	let headerTuples = mergeHeaderTuples(sessionDefaults?.defaultHeaders, config.headers);
	if (serializedBody.contentType && !hasHeaderName(headerTuples, "content-type")) {
		if (!headerTuples) headerTuples = [];
		headerTuples.push(["Content-Type", serializedBody.contentType]);
	}
	const transport = resolveTransportContext(config, sessionDefaults);
	const timeout = config.timeout ?? sessionDefaults?.timeout ?? DEFAULT_REQUEST_TIMEOUT_MS;
	const requestOptions = {
		url,
		method,
		sessionId: sessionContext.sessionId,
		ephemeral: sessionContext.dropAfterRequest
	};
	if (body !== void 0) requestOptions.body = body;
	if (transport.captureDiagnostics !== void 0) requestOptions.captureDiagnostics = transport.captureDiagnostics;
	if (config.onRequestEvent !== void 0) requestOptions.onRequestEvent = config.onRequestEvent;
	if (transport.transportId) requestOptions.transportId = transport.transportId;
	else {
		if (transport.mode !== void 0) applyNativeEmulationMode(requestOptions, transport.mode);
		if (transport.proxy !== void 0) requestOptions.proxy = transport.proxy;
		if (transport.insecure !== void 0) requestOptions.insecure = transport.insecure;
		if (transport.trustStore !== void 0) requestOptions.trustStore = transport.trustStore;
	}
	requestOptions.timeout = timeout;
	if (config.redirect !== void 0) requestOptions.redirect = config.redirect;
	if (config.disableDefaultHeaders !== void 0) requestOptions.disableDefaultHeaders = config.disableDefaultHeaders;
	if (config.compress !== void 0) requestOptions.compress = config.compress;
	if (headerTuples && headerTuples.length > 0) requestOptions.headers = headerTuples;
	return dispatchRequest(requestOptions, url, config.signal ?? null);
}
async function createTransport(options) {
	const mode = resolveEmulationMode(options?.browser, options?.os, options?.emulation);
	validateTrustStore(options?.trustStore);
	if (options?.resolve !== void 0) validateResolve(options.resolve);
	if (options?.poolIdleTimeout !== void 0) validatePositiveNumber(options.poolIdleTimeout, "poolIdleTimeout");
	if (options?.poolMaxIdlePerHost !== void 0) validateNonNegativeInteger(options.poolMaxIdlePerHost, "poolMaxIdlePerHost");
	if (options?.poolMaxSize !== void 0) validatePositiveInteger(options.poolMaxSize, "poolMaxSize");
	if (options?.connectTimeout !== void 0) validatePositiveNumber(options.connectTimeout, "connectTimeout");
	if (options?.readTimeout !== void 0) validatePositiveNumber(options.readTimeout, "readTimeout");
	try {
		const transportOptions = {
			...options?.proxy !== void 0 && { proxy: options.proxy },
			...options?.insecure !== void 0 && { insecure: options.insecure },
			trustStore: options?.trustStore ?? DEFAULT_TRUST_STORE,
			...options?.resolve !== void 0 && { resolve: options.resolve },
			...options?.poolIdleTimeout !== void 0 && { poolIdleTimeout: options.poolIdleTimeout },
			...options?.poolMaxIdlePerHost !== void 0 && { poolMaxIdlePerHost: options.poolMaxIdlePerHost },
			...options?.poolMaxSize !== void 0 && { poolMaxSize: options.poolMaxSize },
			...options?.connectTimeout !== void 0 && { connectTimeout: options.connectTimeout },
			...options?.readTimeout !== void 0 && { readTimeout: options.readTimeout },
			...options?.captureDiagnostics !== void 0 && { captureDiagnostics: options.captureDiagnostics }
		};
		applyNativeEmulationMode(transportOptions, mode);
		return new Transport(nativeBinding.createTransport(transportOptions));
	} catch (error) {
		throw new RequestError(String(error));
	}
}
async function createSession(options) {
	const { sessionId, defaults } = normalizeSessionOptions(options);
	let createdId;
	let transportId;
	try {
		const transportOptions = {
			...defaults.proxy !== void 0 && { proxy: defaults.proxy },
			...defaults.insecure !== void 0 && { insecure: defaults.insecure },
			trustStore: defaults.trustStore ?? DEFAULT_TRUST_STORE,
			...defaults.captureDiagnostics !== void 0 && { captureDiagnostics: defaults.captureDiagnostics }
		};
		applyNativeEmulationMode(transportOptions, defaults.transportMode);
		transportId = nativeBinding.createTransport(transportOptions);
	} catch (error) {
		throw new RequestError(String(error));
	}
	try {
		createdId = nativeBinding.createSession({ sessionId });
	} catch (error) {
		try {
			nativeBinding.dropTransport(transportId);
		} catch {}
		throw new RequestError(String(error));
	}
	defaults.transportId = transportId;
	defaults.ownsTransport = true;
	return new Session(createdId, defaults);
}
async function withSession(fn, options) {
	const session = await createSession(options);
	try {
		return await fn(session);
	} finally {
		await session.close();
	}
}
/**
* @deprecated Use {@link fetch} instead.
*/
async function request(options) {
	if (!options.url) throw new RequestError("URL is required");
	const { url, ...rest } = options;
	const init = {};
	const legacy = rest;
	if (rest.method !== void 0) init.method = rest.method;
	if (rest.headers !== void 0) init.headers = rest.headers;
	if (rest.body !== void 0) init.body = rest.body;
	if (rest.browser !== void 0) init.browser = rest.browser;
	if (rest.os !== void 0) init.os = rest.os;
	if (rest.emulation !== void 0) init.emulation = rest.emulation;
	if (rest.proxy !== void 0) init.proxy = rest.proxy;
	if (rest.timeout !== void 0) init.timeout = rest.timeout;
	if (rest.sessionId !== void 0) init.sessionId = rest.sessionId;
	if (rest.transport !== void 0) init.transport = rest.transport;
	if (rest.insecure !== void 0) init.insecure = rest.insecure;
	if (rest.trustStore !== void 0) init.trustStore = rest.trustStore;
	if (rest.disableDefaultHeaders !== void 0) init.disableDefaultHeaders = rest.disableDefaultHeaders;
	if (rest.redirect !== void 0) init.redirect = rest.redirect;
	if (legacy.signal !== void 0) init.signal = legacy.signal;
	if (legacy.session !== void 0) init.session = legacy.session;
	if (legacy.cookieMode !== void 0) init.cookieMode = legacy.cookieMode;
	else if (legacy.ephemeral === true) init.cookieMode = "ephemeral";
	if (legacy.onRequestEvent !== void 0) init.onRequestEvent = legacy.onRequestEvent;
	if (legacy.captureDiagnostics !== void 0) init.captureDiagnostics = legacy.captureDiagnostics;
	return fetch(url, init);
}
/**
* Get list of available browser profiles
*
* @returns Array of browser profile names
*
* @example
* ```typescript
* import { getProfiles } from 'wreq-js';
*
* const profiles = getProfiles();
* console.log(profiles); // ['chrome_131', 'chrome_142', 'firefox_135', 'safari_18', ...]
* ```
*/
function getProfiles() {
	if (!cachedProfiles) cachedProfiles = nativeBinding.getProfiles();
	return cachedProfiles;
}
function getProfileSet() {
	if (!cachedProfileSet) cachedProfileSet = new Set(getProfiles());
	return cachedProfileSet;
}
/**
* Get list of supported operating systems for emulation.
*
* @returns Array of operating system identifiers
*/
function getOperatingSystems() {
	if (!cachedOperatingSystems) {
		const fromNative = nativeBinding.getOperatingSystems?.();
		cachedOperatingSystems = fromNative && fromNative.length > 0 ? fromNative : [...SUPPORTED_OSES];
	}
	return cachedOperatingSystems;
}
function getOperatingSystemSet() {
	if (!cachedOperatingSystemSet) cachedOperatingSystemSet = new Set(getOperatingSystems());
	return cachedOperatingSystemSet;
}
/**
* Resolve a browser family alias to the concrete profile it points at.
*
* Concrete profiles pass through unchanged, so this is safe to call on any value
* accepted by the `browser` option.
*
* Aliases track the newest profile shipped with the installed version of this library,
* so the result moves when you upgrade. Log it if you need to know which fingerprint a
* given run used.
*
* @param browser - A browser profile or family alias
* @returns The concrete browser profile
*
* @example
* ```typescript
* import { resolveProfile } from 'wreq-js';
*
* resolveProfile('firefox');     // 'firefox_149'
* resolveProfile('firefox_143'); // 'firefox_143'
* ```
*/
function resolveProfile(browser) {
	validateBrowserProfile(browser);
	return Object.hasOwn(BROWSER_ALIASES, browser) ? BROWSER_ALIASES[browser] : browser;
}
/**
* Get the headers a browser profile injects into every request, without sending one.
*
* Returns the emulation's own headers in profile order and original casing. Pass any of
* these back through `headers` to override them for a request; the rest of the profile
* stays intact.
*
* @param browser - Browser profile or family alias to inspect (defaults to the same profile as {@link fetch})
* @param os - Operating system to emulate (defaults to the same OS as {@link fetch})
* @returns A {@link Headers} instance holding the profile's default headers
*
* @example
* ```typescript
* import { fetch, getEmulationHeaders } from 'wreq-js';
*
* const defaults = getEmulationHeaders('firefox_147');
* const userAgent = defaults.get('user-agent');
*
* // Reuse the profile's User-Agent while sending your own headers
* await fetch('https://example.com', {
*   browser: 'firefox_147',
*   headers: { 'X-Client': `proxy (${userAgent})` },
* });
* ```
*/
function getEmulationHeaders(browser, os) {
	validateOperatingSystem(os);
	const resolvedBrowser = browser === void 0 ? DEFAULT_BROWSER : resolveProfile(browser);
	const resolvedOs = os ?? DEFAULT_OS;
	const cacheKey = `${resolvedBrowser} ${resolvedOs}`;
	let tuples = cachedEmulationHeaders.get(cacheKey);
	if (!tuples) {
		const readEmulationHeaders = nativeBinding.getEmulationHeaders;
		if (!readEmulationHeaders) throw new RequestError("getEmulationHeaders is not available in this build of the native addon");
		tuples = readEmulationHeaders(resolvedBrowser, resolvedOs);
		cachedEmulationHeaders.set(cacheKey, tuples);
	}
	return new Headers(tuples);
}
/**
* Convenience helper for GET requests using {@link fetch}.
*/
async function get(url, init) {
	const config = {};
	if (init) Object.assign(config, init);
	config.method = "GET";
	return fetch(url, config);
}
/**
* Convenience helper for POST requests using {@link fetch}.
*/
async function post(url, body, init) {
	const config = {};
	if (init) Object.assign(config, init);
	config.method = "POST";
	if (body !== void 0) config.body = body;
	return fetch(url, config);
}
function normalizeWebSocketUrl(url) {
	const normalized = String(url).trim();
	if (!normalized) throw new RequestError("URL is required");
	let parsed;
	try {
		parsed = new URL(normalized);
	} catch (error) {
		throw new RequestError(String(error));
	}
	if (parsed.hash) throw new RequestError("WebSocket URL must not include a hash fragment");
	if (parsed.protocol === "http:") parsed.protocol = "ws:";
	else if (parsed.protocol === "https:") parsed.protocol = "wss:";
	if (parsed.protocol !== "ws:" && parsed.protocol !== "wss:") throw new RequestError("expected a ws: or wss: url");
	return parsed.toString();
}
function validateWebSocketProtocols(protocols) {
	if (protocols === void 0) return;
	const protocolList = typeof protocols === "string" ? [protocols] : protocols;
	const seen = /* @__PURE__ */ new Set();
	const validToken = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;
	for (const protocol of protocolList) {
		if (typeof protocol !== "string" || protocol.length === 0) throw new RequestError("WebSocket protocol values must be non-empty strings");
		if (!validToken.test(protocol)) throw new RequestError(`Invalid WebSocket protocol value: ${protocol}`);
		if (seen.has(protocol)) throw new RequestError(`Duplicate WebSocket protocol: ${protocol}`);
		seen.add(protocol);
	}
}
function normalizeWebSocketSizeOption(value, label) {
	if (value === void 0) return;
	if (!Number.isSafeInteger(value) || value <= 0) throw new RequestError(`${label} must be a positive safe integer`);
	return value;
}
function normalizeStandaloneWebSocketOptions(options) {
	const normalized = {};
	if (!options) return normalized;
	if (options.browser !== void 0) normalized.browser = options.browser;
	if (options.os !== void 0) normalized.os = options.os;
	if (options.emulation !== void 0) normalized.emulation = options.emulation;
	if (options.headers !== void 0) normalized.headers = options.headers;
	if (options.proxy !== void 0) normalized.proxy = options.proxy;
	if (options.protocols !== void 0) normalized.protocols = options.protocols;
	if (options.maxFrameSize !== void 0) {
		const maxFrameSize = normalizeWebSocketSizeOption(options.maxFrameSize, "maxFrameSize");
		if (maxFrameSize !== void 0) normalized.maxFrameSize = maxFrameSize;
	}
	if (options.maxMessageSize !== void 0) {
		const maxMessageSize = normalizeWebSocketSizeOption(options.maxMessageSize, "maxMessageSize");
		if (maxMessageSize !== void 0) normalized.maxMessageSize = maxMessageSize;
	}
	if (options.binaryType !== void 0) {
		if (options.binaryType !== "nodebuffer" && options.binaryType !== "arraybuffer" && options.binaryType !== "blob") throw new RequestError("binaryType must be one of: 'nodebuffer', 'arraybuffer', 'blob'");
		normalized.binaryType = options.binaryType;
	}
	return normalized;
}
function normalizeSessionWebSocketOptions(options) {
	const normalized = {};
	if (!options) return normalized;
	const optionsWithOverrides = options;
	if (optionsWithOverrides.browser !== void 0) throw new RequestError("`browser` is not supported in session.websocket(); the session controls browser emulation.");
	if (optionsWithOverrides.os !== void 0) throw new RequestError("`os` is not supported in session.websocket(); the session controls OS emulation.");
	if (optionsWithOverrides.emulation !== void 0) throw new RequestError("`emulation` is not supported in session.websocket(); the session transport controls emulation.");
	if (optionsWithOverrides.proxy !== void 0) throw new RequestError("`proxy` is not supported in session.websocket(); the session transport controls proxying.");
	if (options.headers !== void 0) normalized.headers = options.headers;
	if (options.protocols !== void 0) normalized.protocols = options.protocols;
	if (options.maxFrameSize !== void 0) {
		const maxFrameSize = normalizeWebSocketSizeOption(options.maxFrameSize, "maxFrameSize");
		if (maxFrameSize !== void 0) normalized.maxFrameSize = maxFrameSize;
	}
	if (options.maxMessageSize !== void 0) {
		const maxMessageSize = normalizeWebSocketSizeOption(options.maxMessageSize, "maxMessageSize");
		if (maxMessageSize !== void 0) normalized.maxMessageSize = maxMessageSize;
	}
	if (options.binaryType !== void 0) {
		if (options.binaryType !== "nodebuffer" && options.binaryType !== "arraybuffer" && options.binaryType !== "blob") throw new RequestError("binaryType must be one of: 'nodebuffer', 'arraybuffer', 'blob'");
		normalized.binaryType = options.binaryType;
	}
	return normalized;
}
function extractLegacyWebSocketCallbacks(options) {
	if (!isPlainObject(options)) return;
	const maybeCallbacks = options;
	const callbacks = {};
	if (typeof maybeCallbacks.onMessage === "function") callbacks.onMessage = maybeCallbacks.onMessage;
	if (typeof maybeCallbacks.onClose === "function") callbacks.onClose = maybeCallbacks.onClose;
	if (typeof maybeCallbacks.onError === "function") callbacks.onError = maybeCallbacks.onError;
	return Object.keys(callbacks).length > 0 ? callbacks : void 0;
}
function normalizeWebSocketCloseOptions(code, reason) {
	if (code === void 0 && reason === void 0) return;
	if (code === void 0) throw new RequestError("A close code is required when providing a close reason");
	if (!Number.isInteger(code)) throw new RequestError("Close code must be an integer");
	if (code !== 1e3 && (code < 3e3 || code > 4999)) throw new RequestError("Close code must be 1000 or in range 3000-4999");
	const normalizedReason = reason ?? "";
	if (Buffer.byteLength(normalizedReason, "utf8") > 123) throw new RequestError("Close reason must be 123 bytes or fewer");
	return {
		code,
		reason: normalizedReason
	};
}
function isWebSocketListenerType(type) {
	return type === "open" || type === "message" || type === "close" || type === "error";
}
/**
* WHATWG-style WebSocket API with async connection establishment.
*/
var WebSocket = class WebSocket {
	static CONNECTING = 0;
	static OPEN = 1;
	static CLOSING = 2;
	static CLOSED = 3;
	url;
	protocol = "";
	extensions = "";
	readyState = WebSocket.CONNECTING;
	_binaryType = "nodebuffer";
	_bufferedAmount = 0;
	_onopen = null;
	_onmessage = null;
	_onclose = null;
	_onerror = null;
	_onHandlerOrder = {
		open: -1,
		message: -1,
		close: -1,
		error: -1
	};
	_listenerOrderCounter = 0;
	_listeners = {
		open: /* @__PURE__ */ new Map(),
		message: /* @__PURE__ */ new Map(),
		close: /* @__PURE__ */ new Map(),
		error: /* @__PURE__ */ new Map()
	};
	_legacyCallbacks;
	_openDispatchMode;
	_connection;
	_connectPromise;
	_closeOptions;
	_finalizerToken;
	_openEventDispatched = false;
	_openEventQueued = false;
	_closeEventDispatched = false;
	_nativeCloseStarted = false;
	_pendingMessages = [];
	_sendChain = Promise.resolve();
	constructor(urlOrInit, protocolsOrOptions, maybeOptions) {
		let init;
		if (isInternalWebSocketInit(urlOrInit)) init = urlOrInit;
		else init = WebSocket.buildStandaloneInit(urlOrInit, protocolsOrOptions, maybeOptions);
		this.url = init.url;
		this.binaryType = init.options.binaryType ?? "nodebuffer";
		this._legacyCallbacks = init.legacyCallbacks;
		this._openDispatchMode = init.openDispatchMode;
		this._connectPromise = this.connect(init.connect);
		this._connectPromise.catch(() => void 0);
	}
	get binaryType() {
		return this._binaryType;
	}
	set binaryType(value) {
		if (value === "arraybuffer" || value === "blob" || value === "nodebuffer") this._binaryType = value;
	}
	get bufferedAmount() {
		return this._bufferedAmount;
	}
	get onopen() {
		return this._onopen;
	}
	set onopen(listener) {
		this._onopen = listener;
		this._onHandlerOrder.open = listener ? ++this._listenerOrderCounter : -1;
	}
	get onmessage() {
		return this._onmessage;
	}
	set onmessage(listener) {
		this._onmessage = listener;
		this._onHandlerOrder.message = listener ? ++this._listenerOrderCounter : -1;
	}
	get onclose() {
		return this._onclose;
	}
	set onclose(listener) {
		this._onclose = listener;
		this._onHandlerOrder.close = listener ? ++this._listenerOrderCounter : -1;
	}
	get onerror() {
		return this._onerror;
	}
	set onerror(listener) {
		this._onerror = listener;
		this._onHandlerOrder.error = listener ? ++this._listenerOrderCounter : -1;
	}
	static async _connectWithInit(init) {
		const ws = new WebSocket(init);
		await ws._waitUntilConnected();
		ws.scheduleOpenEventAfterAwait();
		return ws;
	}
	static buildStandaloneInit(url, protocolsOrOptions, maybeOptions) {
		const optionsCandidate = typeof protocolsOrOptions === "string" || Array.isArray(protocolsOrOptions) ? maybeOptions : protocolsOrOptions ?? maybeOptions;
		const normalizedOptions = normalizeStandaloneWebSocketOptions(optionsCandidate);
		validateWebSocketProtocols(typeof protocolsOrOptions === "string" || Array.isArray(protocolsOrOptions) ? protocolsOrOptions : normalizedOptions.protocols);
		assertNoManualWebSocketProtocolHeader(normalizedOptions.headers);
		const emulationMode = resolveEmulationMode(normalizedOptions.browser, normalizedOptions.os, normalizedOptions.emulation);
		const protocols = normalizeWebSocketProtocolList(typeof protocolsOrOptions === "string" || Array.isArray(protocolsOrOptions) ? protocolsOrOptions : normalizedOptions.protocols);
		return {
			_internal: true,
			url: normalizeWebSocketUrl(url),
			options: normalizedOptions,
			openDispatchMode: "automatic",
			connect: (callbacks) => {
				const nativeOptions = {
					url: normalizeWebSocketUrl(url),
					headers: headersToTuples(normalizedOptions.headers ?? {}),
					...protocols && protocols.length > 0 && { protocols },
					...normalizedOptions.proxy !== void 0 && { proxy: normalizedOptions.proxy },
					...normalizedOptions.maxFrameSize !== void 0 && { maxFrameSize: normalizedOptions.maxFrameSize },
					...normalizedOptions.maxMessageSize !== void 0 && { maxMessageSize: normalizedOptions.maxMessageSize },
					onMessage: callbacks.onMessage,
					onClose: callbacks.onClose,
					onError: callbacks.onError
				};
				applyNativeEmulationMode(nativeOptions, emulationMode);
				return nativeBinding.websocketConnect(nativeOptions);
			},
			legacyCallbacks: extractLegacyWebSocketCallbacks(optionsCandidate)
		};
	}
	async connect(connectFn) {
		try {
			const connection = await connectFn({
				onMessage: (data) => {
					this.handleNativeMessage(data);
				},
				onClose: (event) => {
					this.handleNativeClose(event);
				},
				onError: (message) => {
					this.handleNativeError(message);
				}
			});
			this._connection = connection;
			this.protocol = connection.protocol ?? "";
			this.extensions = connection.extensions ?? "";
			if (websocketFinalizer) {
				this._finalizerToken = connection;
				websocketFinalizer.register(this, connection, connection);
			}
			if (this.readyState === WebSocket.CLOSING) {
				this.startNativeClose();
				return;
			}
			this.readyState = WebSocket.OPEN;
			if (this._openDispatchMode === "automatic") this.scheduleOpenEventAfterConnect();
		} catch (error) {
			this.handleNativeError(String(error));
			this.finalizeClosed({
				code: 1006,
				reason: ""
			}, false);
			throw new RequestError(String(error));
		}
	}
	_waitUntilConnected() {
		return this._connectPromise;
	}
	scheduleOpenEventAfterConnect() {
		this.scheduleOpenEventWithDepth(2);
	}
	scheduleOpenEventAfterAwait() {
		this.scheduleOpenEventWithDepth(3);
	}
	scheduleOpenEventWithDepth(depth) {
		if (this._openEventDispatched || this._openEventQueued || this.readyState !== WebSocket.OPEN) return;
		this._openEventQueued = true;
		const queue = (remaining) => {
			if (remaining === 0) {
				this._openEventQueued = false;
				if (this._openEventDispatched || this.readyState !== WebSocket.OPEN) return;
				this._openEventDispatched = true;
				this.dispatchOpenEvent();
				return;
			}
			queueMicrotask(() => {
				queue(remaining - 1);
			});
		};
		queue(depth);
	}
	releaseConnectionTracking() {
		if (!this._finalizerToken || !websocketFinalizer) return;
		websocketFinalizer.unregister(this._finalizerToken);
		this._finalizerToken = void 0;
	}
	toMessageEventData(data) {
		if (typeof data === "string") return data;
		if (this._binaryType === "arraybuffer") {
			const arrayBuffer = new ArrayBuffer(data.byteLength);
			new Uint8Array(arrayBuffer).set(data);
			return arrayBuffer;
		}
		if (this._binaryType === "blob") return new Blob([data]);
		return data;
	}
	invokeListener(listener, event) {
		try {
			if (typeof listener === "function") listener.call(this, event);
			else listener.handleEvent(event);
		} catch {}
	}
	createBaseEvent(type) {
		return {
			type,
			isTrusted: false,
			timeStamp: Date.now(),
			target: this,
			currentTarget: this
		};
	}
	getOnHandler(type) {
		switch (type) {
			case "open": return this._onopen;
			case "message": return this._onmessage;
			case "close": return this._onclose;
			case "error": return this._onerror;
			default: return null;
		}
	}
	getOnHandlerOrder(type) {
		return this._onHandlerOrder[type];
	}
	getListenerMap(type) {
		return this._listeners[type];
	}
	dispatchEvent(type, event) {
		const listenerMap = this.getListenerMap(type);
		const onHandler = this.getOnHandler(type);
		if (listenerMap.size === 0 && !onHandler) return;
		const ordered = [];
		for (const descriptor of listenerMap.values()) ordered.push({
			order: descriptor.order,
			listener: descriptor.listener,
			once: descriptor.once
		});
		if (onHandler) ordered.push({
			order: this.getOnHandlerOrder(type),
			listener: onHandler,
			once: false
		});
		ordered.sort((a, b) => a.order - b.order);
		for (const entry of ordered) {
			if (entry.once) this.removeEventListener(type, entry.listener);
			this.invokeListener(entry.listener, event);
		}
	}
	dispatchOpenEvent() {
		const event = this.createBaseEvent("open");
		this.dispatchEvent("open", event);
		if (!this._closeEventDispatched && this._pendingMessages.length > 0) {
			const pending = this._pendingMessages;
			this._pendingMessages = [];
			for (const data of pending) this.dispatchMessageEvent(this.toMessageEventData(data));
		}
	}
	dispatchMessageEvent(data) {
		const event = {
			...this.createBaseEvent("message"),
			data
		};
		this.dispatchEvent("message", event);
	}
	dispatchCloseEvent(event) {
		this.dispatchEvent("close", event);
	}
	dispatchErrorEvent(message) {
		const event = {
			...this.createBaseEvent("error"),
			...message !== void 0 && { message }
		};
		this.dispatchEvent("error", event);
	}
	handleNativeMessage(data) {
		if (this._closeEventDispatched) return;
		this._legacyCallbacks?.onMessage?.(data);
		if (!this._openEventDispatched && this.readyState === WebSocket.OPEN) {
			this._pendingMessages.push(data);
			return;
		}
		this.dispatchMessageEvent(this.toMessageEventData(data));
	}
	handleNativeError(message) {
		this._legacyCallbacks?.onError?.(message);
		this.dispatchErrorEvent(message);
	}
	handleNativeClose(event) {
		const wasClean = this.readyState === WebSocket.CLOSING || event.code === 1e3;
		this.finalizeClosed(event, wasClean);
	}
	finalizeClosed(event, wasClean) {
		if (this._closeEventDispatched) return;
		this.readyState = WebSocket.CLOSED;
		this._closeEventDispatched = true;
		this._pendingMessages = [];
		this.releaseConnectionTracking();
		const closeEvent = {
			...this.createBaseEvent("close"),
			code: event.code,
			reason: event.reason,
			wasClean
		};
		this._legacyCallbacks?.onClose?.(closeEvent);
		this.dispatchCloseEvent(closeEvent);
	}
	startNativeClose() {
		if (this._nativeCloseStarted || !this._connection) return;
		this._nativeCloseStarted = true;
		const connection = this._connection;
		const closeOptions = this._closeOptions;
		nativeBinding.websocketClose(connection, closeOptions).catch((error) => {
			this.handleNativeError(String(error));
			this.finalizeClosed({
				code: 1006,
				reason: ""
			}, false);
		});
	}
	addEventListener(type, listener, options) {
		if (!listener || !isWebSocketListenerType(type)) return;
		const normalizedListener = listener;
		if (typeof normalizedListener !== "function" && (typeof normalizedListener !== "object" || normalizedListener === null || typeof normalizedListener.handleEvent !== "function")) return;
		const listenerMap = this.getListenerMap(type);
		if (listenerMap.has(normalizedListener)) return;
		const parsedOptions = typeof options === "boolean" ? {} : options ?? {};
		const once = parsedOptions.once === true;
		const signal = parsedOptions.signal;
		if (signal?.aborted) return;
		const descriptor = {
			listener: normalizedListener,
			order: ++this._listenerOrderCounter,
			once
		};
		if (signal) {
			const onAbort = () => {
				this.removeEventListener(type, normalizedListener);
			};
			descriptor.abortSignal = signal;
			descriptor.abortHandler = onAbort;
			signal.addEventListener("abort", onAbort, { once: true });
		}
		listenerMap.set(normalizedListener, descriptor);
	}
	removeEventListener(type, listener) {
		if (!listener || !isWebSocketListenerType(type)) return;
		const normalizedListener = listener;
		if (typeof normalizedListener !== "function" && typeof normalizedListener !== "object") return;
		const listenerMap = this.getListenerMap(type);
		const descriptor = listenerMap.get(normalizedListener);
		if (!descriptor) return;
		if (descriptor.abortSignal && descriptor.abortHandler) descriptor.abortSignal.removeEventListener("abort", descriptor.abortHandler);
		listenerMap.delete(normalizedListener);
	}
	getSendByteLength(data) {
		if (typeof data === "string") return Buffer.byteLength(data);
		if (Buffer.isBuffer(data)) return data.byteLength;
		if (data instanceof ArrayBuffer) return data.byteLength;
		if (ArrayBuffer.isView(data)) return data.byteLength;
		if (typeof Blob !== "undefined" && data instanceof Blob) return data.size;
		throw new TypeError("WebSocket data must be a string, Buffer, ArrayBuffer, ArrayBufferView, or Blob");
	}
	async normalizeSendPayload(data) {
		if (typeof data === "string") return data;
		if (Buffer.isBuffer(data)) return data;
		if (data instanceof ArrayBuffer) return Buffer.from(data);
		if (ArrayBuffer.isView(data)) return Buffer.from(data.buffer, data.byteOffset, data.byteLength);
		if (typeof Blob !== "undefined" && data instanceof Blob) return Buffer.from(await data.arrayBuffer());
		throw new TypeError("WebSocket data must be a string, Buffer, ArrayBuffer, ArrayBufferView, or Blob");
	}
	send(data) {
		if (this.readyState !== WebSocket.OPEN || !this._connection) throw new RequestError("WebSocket is not open");
		const queuedBytes = this.getSendByteLength(data);
		const connection = this._connection;
		this._bufferedAmount += queuedBytes;
		const sendTask = async () => {
			try {
				const payload = await this.normalizeSendPayload(data);
				await nativeBinding.websocketSend(connection, payload);
			} catch (error) {
				this.handleNativeError(String(error));
				this.finalizeClosed({
					code: 1006,
					reason: ""
				}, false);
			} finally {
				this._bufferedAmount = Math.max(0, this._bufferedAmount - queuedBytes);
			}
		};
		this._sendChain = this._sendChain.then(sendTask, sendTask);
	}
	close(code, reason) {
		if (this.readyState === WebSocket.CLOSING || this.readyState === WebSocket.CLOSED) return;
		this._closeOptions = normalizeWebSocketCloseOptions(code, reason);
		this.readyState = WebSocket.CLOSING;
		this.startNativeClose();
	}
	[Symbol.dispose]() {
		this.close();
	}
};
function isInternalWebSocketInit(value) {
	if (!isPlainObject(value)) return false;
	const candidate = value;
	return candidate._internal === true && typeof candidate.url === "string" && typeof candidate.connect === "function";
}
function normalizeStandaloneWebSocketArgs(urlOrOptions, options) {
	if (typeof urlOrOptions === "string" || urlOrOptions instanceof URL) {
		const normalizedOptions = normalizeStandaloneWebSocketOptions(options);
		return {
			url: normalizeWebSocketUrl(urlOrOptions),
			options: normalizedOptions,
			legacyCallbacks: extractLegacyWebSocketCallbacks(options)
		};
	}
	const legacy = urlOrOptions;
	const normalizedOptions = normalizeStandaloneWebSocketOptions(legacy);
	return {
		url: normalizeWebSocketUrl(legacy.url),
		options: normalizedOptions,
		legacyCallbacks: extractLegacyWebSocketCallbacks(legacy)
	};
}
function normalizeSessionWebSocketArgs(urlOrOptions, options) {
	if (typeof urlOrOptions === "string" || urlOrOptions instanceof URL) {
		const normalizedOptions = normalizeSessionWebSocketOptions(options);
		return {
			url: normalizeWebSocketUrl(urlOrOptions),
			options: normalizedOptions,
			legacyCallbacks: extractLegacyWebSocketCallbacks(options)
		};
	}
	const legacy = urlOrOptions;
	const normalizedOptions = normalizeSessionWebSocketOptions(legacy);
	return {
		url: normalizeWebSocketUrl(legacy.url),
		options: normalizedOptions,
		legacyCallbacks: extractLegacyWebSocketCallbacks(legacy)
	};
}
async function websocket(urlOrOptions, options) {
	const normalized = normalizeStandaloneWebSocketArgs(urlOrOptions, options);
	validateWebSocketProtocols(normalized.options.protocols);
	assertNoManualWebSocketProtocolHeader(normalized.options.headers);
	const emulationMode = resolveEmulationMode(normalized.options.browser, normalized.options.os, normalized.options.emulation);
	const protocols = normalizeWebSocketProtocolList(normalized.options.protocols);
	return WebSocket._connectWithInit({
		_internal: true,
		url: normalized.url,
		options: normalized.options,
		openDispatchMode: "deferred",
		connect: (callbacks) => {
			const nativeOptions = {
				url: normalized.url,
				headers: headersToTuples(normalized.options.headers ?? {}),
				...protocols && protocols.length > 0 && { protocols },
				...normalized.options.proxy !== void 0 && { proxy: normalized.options.proxy },
				...normalized.options.maxFrameSize !== void 0 && { maxFrameSize: normalized.options.maxFrameSize },
				...normalized.options.maxMessageSize !== void 0 && { maxMessageSize: normalized.options.maxMessageSize },
				onMessage: callbacks.onMessage,
				onClose: callbacks.onClose,
				onError: callbacks.onError
			};
			applyNativeEmulationMode(nativeOptions, emulationMode);
			return nativeBinding.websocketConnect(nativeOptions);
		},
		legacyCallbacks: normalized.legacyCallbacks
	});
}
var wreq_js_default = {
	fetch,
	request,
	get,
	post,
	getProfiles,
	getOperatingSystems,
	createTransport,
	createSession,
	withSession,
	websocket,
	WebSocket,
	Headers,
	Response,
	Transport,
	Session,
	RequestError
};
//#endregion
exports.Headers = Headers;
exports.RequestError = RequestError;
exports.Response = Response;
exports.Session = Session;
exports.Transport = Transport;
exports.WebSocket = WebSocket;
exports.createSession = createSession;
exports.createTransport = createTransport;
exports.default = wreq_js_default;
exports.fetch = fetch;
exports.get = get;
exports.getEmulationHeaders = getEmulationHeaders;
exports.getOperatingSystems = getOperatingSystems;
exports.getProfiles = getProfiles;
exports.post = post;
exports.request = request;
exports.resolveProfile = resolveProfile;
exports.websocket = websocket;
exports.withSession = withSession;

//# sourceMappingURL=wreq-js.cjs.map