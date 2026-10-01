import { Readable } from "node:stream";
import { ReadableStream as ReadableStream$1 } from "node:stream/web";
//#region src/generated-types.d.ts
/**
 * Auto-generated from Rust build script
 * DO NOT EDIT MANUALLY
 */
/**
 * Browser profile names supported
 */
type BrowserProfile = 'chrome_100' | 'chrome_101' | 'chrome_104' | 'chrome_105' | 'chrome_106' | 'chrome_107' | 'chrome_108' | 'chrome_109' | 'chrome_110' | 'chrome_114' | 'chrome_116' | 'chrome_117' | 'chrome_118' | 'chrome_119' | 'chrome_120' | 'chrome_123' | 'chrome_124' | 'chrome_126' | 'chrome_127' | 'chrome_128' | 'chrome_129' | 'chrome_130' | 'chrome_131' | 'chrome_132' | 'chrome_133' | 'chrome_134' | 'chrome_135' | 'chrome_136' | 'chrome_137' | 'chrome_138' | 'chrome_139' | 'chrome_140' | 'chrome_141' | 'chrome_142' | 'chrome_143' | 'chrome_144' | 'chrome_145' | 'chrome_146' | 'chrome_147' | 'chrome_148' | 'chrome_149' | 'edge_101' | 'edge_122' | 'edge_127' | 'edge_131' | 'edge_134' | 'edge_135' | 'edge_136' | 'edge_137' | 'edge_138' | 'edge_139' | 'edge_140' | 'edge_141' | 'edge_142' | 'edge_143' | 'edge_144' | 'edge_145' | 'edge_146' | 'edge_147' | 'edge_148' | 'opera_116' | 'opera_117' | 'opera_118' | 'opera_119' | 'opera_120' | 'opera_121' | 'opera_122' | 'opera_123' | 'opera_124' | 'opera_125' | 'opera_126' | 'opera_127' | 'opera_128' | 'opera_129' | 'opera_130' | 'opera_131' | 'firefox_109' | 'firefox_117' | 'firefox_128' | 'firefox_133' | 'firefox_135' | 'firefox_private_135' | 'firefox_android_135' | 'firefox_136' | 'firefox_private_136' | 'firefox_139' | 'firefox_142' | 'firefox_143' | 'firefox_144' | 'firefox_145' | 'firefox_146' | 'firefox_147' | 'firefox_148' | 'firefox_149' | 'firefox_150' | 'firefox_151' | 'safari_ios_17.2' | 'safari_ios_17.4.1' | 'safari_ios_16.5' | 'safari_15.3' | 'safari_15.5' | 'safari_15.6.1' | 'safari_16' | 'safari_16.5' | 'safari_17.0' | 'safari_17.2.1' | 'safari_17.4.1' | 'safari_17.5' | 'safari_17.6' | 'safari_18' | 'safari_ipad_18' | 'safari_18.2' | 'safari_ios_18.1.1' | 'safari_18.3' | 'safari_18.3.1' | 'safari_18.5' | 'safari_26' | 'safari_26.1' | 'safari_26.2' | 'safari_26.3' | 'safari_26.4' | 'safari_ipad_26' | 'safari_ipad_26.2' | 'safari_ios_26' | 'safari_ios_26.2' | 'okhttp_3.9' | 'okhttp_3.11' | 'okhttp_3.13' | 'okhttp_3.14' | 'okhttp_4.9' | 'okhttp_4.10' | 'okhttp_4.12' | 'okhttp_5';
/**
 * Operating systems supported for emulation
 */
type EmulationOS = 'windows' | 'macos' | 'linux' | 'android' | 'ios';
/**
 * Browser family aliases, each resolving to the newest profile in its family
 */
type BrowserAlias = 'chrome' | 'edge' | 'firefox' | 'firefox_android' | 'firefox_private' | 'okhttp' | 'opera' | 'safari' | 'safari_ios' | 'safari_ipad';
//#endregion
//#region src/types.d.ts
/**
 * Controls how cookies are scoped for a request.
 * - "session": reuse an explicit Session or sessionId across calls.
 * - "ephemeral": create an isolated, single-use session.
 */
type CookieMode = "session" | "ephemeral";
/**
 * Minimal handle implemented by {@link Session}. Exposed for integrations
 * that only need to carry a session id.
 */
interface SessionHandle {
  readonly id: string;
}
interface SessionCookie {
  name: string;
  value: string;
  domain?: string;
  path?: string;
  secure: boolean;
  httpOnly: boolean;
  sameSite?: "lax" | "strict" | "none";
  expiresAtMs?: number;
}
/**
 * A tuple of [name, value] pairs used for initializing headers.
 * Both name and value must be strings.
 *
 * @example
 * ```typescript
 * const headers: HeaderTuple = ['Content-Type', 'application/json'];
 * ```
 */
type HeaderTuple = [string, string];
/**
 * Represents various input types accepted when creating or initializing headers.
 * Can be an iterable of header tuples, an array of tuples, or a plain object.
 *
 * @example
 * ```typescript
 * // As an object
 * const headers: HeadersInit = { 'Content-Type': 'application/json' };
 *
 * // As an array of tuples
 * const headers: HeadersInit = [['Content-Type', 'application/json']];
 *
 * // As an iterable
 * const headers: HeadersInit = new Map([['Content-Type', 'application/json']]);
 * ```
 */
type HeadersInit = Iterable<HeaderTuple> | Array<HeaderTuple> | Record<string, string | number | boolean | null | undefined>;
type AlpnProtocol = "HTTP1" | "HTTP2" | "HTTP3";
type AlpsProtocol = "HTTP1" | "HTTP2" | "HTTP3";
type TlsVersion = "1.0" | "1.1" | "1.2" | "1.3" | "TLS1.0" | "TLS1.1" | "TLS1.2" | "TLS1.3";
type Http2PseudoHeaderId = "Method" | "Scheme" | "Authority" | "Path" | "Protocol";
type TrustStoreMode = "combined" | "mozilla" | "defaultPaths";
/**
 * Overrides DNS resolution for specific hosts.
 *
 * Each key is a hostname and each value is one address, or an array of fallback addresses,
 * written as `ip` or `ip:port`. Following reqwest/wreq semantics the port is optional and ignored,
 * the request URL's port is always used.
 *
 * Useful for pinning a host to an address you have already resolved and vetted
 * (for example to enforce SSRF protection), split-horizon DNS, or testing
 * against a local server under a real hostname.
 *
 * @example
 * ```typescript
 * const transport = await createTransport({
 *   resolve: { "example.com": "93.184.216.34" },
 * });
 * ```
 */
type ResolveMap = Record<string, string | string[]>;
type Http2SettingId = "HeaderTableSize" | "EnablePush" | "MaxConcurrentStreams" | "InitialWindowSize" | "MaxFrameSize" | "MaxHeaderListSize" | "EnableConnectProtocol" | "NoRfc7540Priorities";
interface Http2StreamDependency {
  dependencyId: number;
  weight: number;
  exclusive?: boolean;
}
interface Http2Priority {
  streamId: number;
  dependency: Http2StreamDependency;
}
interface CustomTlsOptions {
  alpnProtocols?: AlpnProtocol[];
  alpsProtocols?: AlpsProtocol[];
  alpsUseNewCodepoint?: boolean;
  sessionTicket?: boolean;
  minTlsVersion?: TlsVersion;
  maxTlsVersion?: TlsVersion;
  preSharedKey?: boolean;
  enableEchGrease?: boolean;
  permuteExtensions?: boolean;
  greaseEnabled?: boolean;
  enableOcspStapling?: boolean;
  enableSignedCertTimestamps?: boolean;
  recordSizeLimit?: number;
  pskSkipSessionTicket?: boolean;
  pskDheKe?: boolean;
  renegotiation?: boolean;
  delegatedCredentials?: string;
  curvesList?: string;
  cipherList?: string;
  sigalgsList?: string;
  certificateCompressionAlgorithms?: Array<"zlib" | "brotli" | "zstd">;
  extensionPermutation?: number[];
  aesHwOverride?: boolean;
  preserveTls13CipherList?: boolean;
  randomAesHwOverride?: boolean;
}
interface CustomHttp1Options {
  http09Responses?: boolean;
  writev?: boolean;
  maxHeaders?: number;
  readBufExactSize?: number;
  maxBufSize?: number;
  ignoreInvalidHeadersInResponses?: boolean;
  allowSpacesAfterHeaderNameInResponses?: boolean;
  allowObsoleteMultilineHeadersInResponses?: boolean;
}
interface CustomHttp2Options {
  adaptiveWindow?: boolean;
  initialStreamId?: number;
  initialConnectionWindowSize?: number;
  initialWindowSize?: number;
  initialMaxSendStreams?: number;
  maxFrameSize?: number;
  keepAliveInterval?: number;
  keepAliveTimeout?: number;
  keepAliveWhileIdle?: boolean;
  maxConcurrentResetStreams?: number;
  maxSendBufferSize?: number;
  maxConcurrentStreams?: number;
  maxHeaderListSize?: number;
  maxPendingAcceptResetStreams?: number;
  enablePush?: boolean;
  headerTableSize?: number;
  enableConnectProtocol?: boolean;
  noRfc7540Priorities?: boolean;
  settingsOrder?: Http2SettingId[];
  headersPseudoOrder?: Http2PseudoHeaderId[];
  headersStreamDependency?: Http2StreamDependency;
  priorities?: Http2Priority[];
}
interface CustomEmulationOptions {
  tlsOptions?: CustomTlsOptions;
  http1Options?: CustomHttp1Options;
  http2Options?: CustomHttp2Options;
  headers?: HeadersInit;
  origHeaders?: string[];
}
/**
 * Represents the various types of data that can be used as a request body.
 * Supports strings, binary payloads, URL-encoded parameters, multipart forms, and blobs.
 *
 * @example
 * ```typescript
 * // String body
 * const body: BodyInit = JSON.stringify({ key: 'value' });
 *
 * // URLSearchParams
 * const body: BodyInit = new URLSearchParams({ key: 'value' });
 *
 * // Buffer
 * const body: BodyInit = Buffer.from('data');
 *
 * // FormData
 * const body: BodyInit = new FormData();
 *
 * // ReadableStream (buffered before sending, not streamed)
 * const body: BodyInit = stream;
 * ```
 */
type BodyInit = string | ArrayBuffer | ArrayBufferView | URLSearchParams | Buffer | Blob | FormData | ReadableStream<Uint8Array> | AsyncIterable<Uint8Array> | Iterable<Uint8Array>;
/**
 * Details about why a WebSocket connection closed.
 */
type WebSocketBinaryType = "nodebuffer" | "arraybuffer" | "blob";
type WebSocketEventType = "open" | "message" | "close" | "error";
interface WebSocketOpenEvent {
  type: "open";
  isTrusted: false;
  timeStamp: number;
  target: WebSocket;
  currentTarget: WebSocket;
}
interface WebSocketMessageEvent {
  type: "message";
  isTrusted: false;
  timeStamp: number;
  data: string | Buffer | ArrayBuffer | Blob;
  target: WebSocket;
  currentTarget: WebSocket;
}
interface WebSocketErrorEvent {
  type: "error";
  isTrusted: false;
  timeStamp: number;
  message?: string;
  target: WebSocket;
  currentTarget: WebSocket;
}
interface WebSocketCloseEvent {
  type: "close";
  isTrusted: false;
  timeStamp: number;
  /**
   * WebSocket close status code (RFC 6455).
   */
  code: number;
  /**
   * UTF-8 close reason sent by the peer.
   */
  reason: string;
  wasClean: boolean;
  target: WebSocket;
  currentTarget: WebSocket;
}
/**
 * Options for configuring a fetch style request with wreq-specific extensions
 * for browser impersonation, proxies, sessions, and timeouts.
 *
 * @example
 * ```typescript
 * const options: RequestInit = {
 *   method: 'POST',
 *   headers: { 'Content-Type': 'application/json' },
 *   body: JSON.stringify({ key: 'value' }),
 *   browser: 'chrome_142',
 *   proxy: 'http://proxy.example.com:8080',
 *   timeout: 5000
 * };
 * ```
 */
type RequestEventType = "request_start" | "request_sent" | "response_headers" | "body_progress" | "body_complete" | "done" | "error";
interface RequestEvent {
  type: RequestEventType;
  timestamp: number;
  status?: number;
  url?: string;
  contentLength?: number | null;
  downloadedBytes?: number;
  message?: string;
}
interface RequestDiagnostics {
  totalDurationMs?: number;
  headersDurationMs?: number;
  status?: number;
  localAddr?: string;
  remoteAddr?: string;
  tlsPeerCertificatePresent?: boolean;
  tlsPeerCertificateChainLength?: number;
}
interface RequestInit {
  /**
   * A string to set request's method.
   * @default 'GET'
   */
  method?: string;
  /**
   * A Headers object, an object literal, or an array of two-item arrays to set request's headers.
   */
  headers?: HeadersInit;
  /**
   * A BodyInit object or null to set request's body.
   *
   * Stream and iterable bodies are read fully into memory before the request is sent,
   * so an unbounded stream means an unbounded allocation.
   */
  body?: BodyInit | null;
  /**
   * Accepted for compatibility with the Fetch API, where it is required alongside a
   * stream body. Stream bodies are buffered here, which satisfies half-duplex on its
   * own, so this option has no effect.
   */
  duplex?: "half";
  /**
   * An AbortSignal to set request's signal.
   */
  signal?: AbortSignal | null;
  /**
   * A string indicating whether request follows redirects, results in an error upon
   * encountering a redirect, or returns the redirect (in an opaque fashion).
   * @default 'follow'
   */
  redirect?: "follow" | "manual" | "error";
  /**
   * Transport instance to use for this request. When provided, transport-level
   * options such as `browser`, `os`, `proxy`, and `insecure` must not be set.
   */
  transport?: Transport;
  /**
   * Browser profile to impersonate for this request.
   * Applies browser profile behavior handled by the native layer.
   * Ignored when `transport` is provided.
   *
   * Accepts a concrete profile ('firefox_149') or a family alias ('firefox'),
   * which resolves to the newest profile in that family.
   * @default 'chrome'
   */
  browser?: BrowserProfile | BrowserAlias;
  /**
   * Operating system to emulate for this request.
   * Influences platform-specific behavior handled by the native layer.
   * Ignored when `transport` is provided.
   * @default 'macos'
   */
  os?: EmulationOS;
  /**
   * Custom emulation overrides. When `browser` or `os` is present, these fields
   * layer on top of the resolved preset profile. When both are omitted, this
   * switches the request into standalone custom emulation mode.
   */
  emulation?: CustomEmulationOptions;
  /**
   * Proxy URL to route the request through (e.g., 'http://proxy.example.com:8080').
   * Proxy support depends on the native layer and proxy scheme.
   * Ignored when `transport` is provided.
   */
  proxy?: string;
  /**
   * Request timeout in milliseconds. If the request takes longer than this value,
   * it will be aborted.
   * @default 30000
   */
  timeout?: number;
  /**
   * Controls how cookies are managed for this call.
   * - "ephemeral": default when no session/sessionId is provided. Creates an isolated session per request.
   * - "session": requires an explicit session or sessionId and reuses its cookie jar.
   */
  cookieMode?: CookieMode;
  /**
   * Session instance to bind this request to. When provided, {@link cookieMode}
   * automatically behaves like `"session"`.
   */
  session?: Session;
  /**
   * Identifier of an existing session created elsewhere (e.g., via {@link createSession}).
   */
  sessionId?: string;
  /**
   * Disable default headers from browser emulation. When enabled, only explicitly
   * provided headers will be sent with the request, preventing emulation headers
   * from being automatically added or appended.
   * @default false
   */
  disableDefaultHeaders?: boolean;
  /**
   * Disable HTTPS certificate verification. When enabled, self-signed and invalid
   * certificates will be accepted.
   * Ignored when `transport` is provided.
   *
   * # Warning
   *
   * You should think very carefully before using this method. If invalid
   * certificates are trusted, *any* certificate for *any* site will be
   * trusted for use. This includes expired certificates. This introduces
   * significant vulnerabilities, and should only be used as a last resort.
   *
   * @default false
   */
  insecure?: boolean;
  /**
   * Controls which certificate roots are used for TLS verification.
   *
   * - `"combined"` merges OpenSSL/BoringSSL default verify paths with bundled Mozilla roots.
   * - `"mozilla"` uses only the bundled Mozilla roots.
   * - `"defaultPaths"` uses only OpenSSL/BoringSSL default verify paths such as
   *   `SSL_CERT_FILE` / `SSL_CERT_DIR` when configured.
   *
   * Note: `"defaultPaths"` is not a native OS trust-store verifier.
   * Ignored when `transport` is provided.
   * @default "combined"
   */
  trustStore?: TrustStoreMode;
  /**
   * Whether to automatically decompress response bodies. When set to `false`,
   * the raw compressed response body is returned as-is and the `Content-Encoding`
   * header is preserved. Useful for proxy scenarios where the downstream client
   * handles decompression.
   * @default true
   */
  compress?: boolean;
  /**
   * Optional callback for structured request lifecycle events emitted by the
   * native bridge.
   */
  onRequestEvent?: (event: RequestEvent) => void;
  /**
   * Capture a final diagnostics payload where supported by the native layer.
   */
  captureDiagnostics?: boolean;
}
/**
 * Configuration for {@link createSession}.
 */
interface CreateSessionOptions {
  /**
   * Provide a custom identifier instead of an auto-generated random ID.
   */
  sessionId?: string;
  /**
   * Default headers applied to every request made through this session.
   */
  defaultHeaders?: HeadersInit;
  /**
   * Browser profile to bind to this session. Defaults to 'chrome'.
   *
   * Accepts a concrete profile ('firefox_149') or a family alias ('firefox'),
   * which resolves to the newest profile in that family.
   */
  browser?: BrowserProfile | BrowserAlias;
  /**
   * Operating system to bind to this session. Defaults to 'macos'.
   */
  os?: EmulationOS;
  /**
   * Custom emulation overrides or a standalone custom emulation for the session transport.
   */
  emulation?: CustomEmulationOptions;
  /**
   * Optional proxy for every request made through the session.
   */
  proxy?: string;
  /**
   * Default timeout applied when {@link Session.fetch} is called without
   * overriding `timeout`.
   */
  timeout?: number;
  /**
   * Disable HTTPS certificate verification. When enabled, self-signed and invalid
   * certificates will be accepted for all requests made through this session.
   *
   * # Warning
   *
   * You should think very carefully before using this method. If invalid
   * certificates are trusted, *any* certificate for *any* site will be
   * trusted for use. This includes expired certificates. This introduces
   * significant vulnerabilities, and should only be used as a last resort.
   *
   * @default false
   */
  insecure?: boolean;
  /**
   * Controls which certificate roots are used for TLS verification for this session.
   * @default "combined"
   */
  trustStore?: TrustStoreMode;
  /**
   * Enable extra connection/TLS diagnostics for requests made through this session.
   */
  captureDiagnostics?: boolean;
}
/**
 * Configuration for {@link createTransport}.
 */
interface CreateTransportOptions {
  /**
   * Proxy URL to route requests through (e.g., 'http://proxy.example.com:8080').
   */
  proxy?: string;
  /**
   * Browser profile to impersonate for this transport.
   *
   * Accepts a concrete profile ('firefox_149') or a family alias ('firefox'),
   * which resolves to the newest profile in that family.
   */
  browser?: BrowserProfile | BrowserAlias;
  /**
   * Operating system to emulate for this transport.
   */
  os?: EmulationOS;
  /**
   * Custom emulation overrides or a standalone custom emulation for this transport.
   */
  emulation?: CustomEmulationOptions;
  /**
   * Disable HTTPS certificate verification for this transport.
   */
  insecure?: boolean;
  /**
   * Controls which certificate roots are used for TLS verification for this transport.
   * @default "combined"
   */
  trustStore?: TrustStoreMode;
  /**
   * Override DNS resolution for specific hosts. See {@link ResolveMap}.
   */
  resolve?: ResolveMap;
  /**
   * Idle timeout for pooled connections (ms).
   */
  poolIdleTimeout?: number;
  /**
   * Maximum number of idle connections per host.
   */
  poolMaxIdlePerHost?: number;
  /**
   * Maximum total connections in the pool.
   */
  poolMaxSize?: number;
  /**
   * TCP connect timeout (ms).
   */
  connectTimeout?: number;
  /**
   * Read timeout (ms).
   */
  readTimeout?: number;
  /**
   * Enable extra connection/TLS diagnostics for requests made through this transport.
   */
  captureDiagnostics?: boolean;
}
/**
 * Legacy request options interface. This interface is deprecated and will be removed in a future version.
 *
 * @deprecated Use {@link RequestInit} with the standard `fetch()` API instead.
 *
 * @example
 * ```typescript
 * // Old (deprecated):
 * const options: RequestOptions = {
 *   url: 'https://api.example.com',
 *   method: 'POST',
 *   body: JSON.stringify({ data: 'value' })
 * };
 *
 * // New (recommended):
 * const response = await fetch('https://api.example.com', {
 *   method: 'POST',
 *   body: JSON.stringify({ data: 'value' })
 * });
 * ```
 */
interface RequestOptions {
  /**
   * The URL to request.
   */
  url: string;
  /**
   * Browser profile to impersonate.
   * Applies browser profile behavior handled by the native layer.
   *
   * Accepts a concrete profile ('firefox_149') or a family alias ('firefox'),
   * which resolves to the newest profile in that family.
   * @default 'chrome'
   */
  browser?: BrowserProfile | BrowserAlias;
  /**
   * Operating system to emulate.
   * @default 'macos'
   */
  os?: EmulationOS;
  /**
   * Custom emulation overrides. When `browser` or `os` is present, these fields
   * layer on top of the resolved preset profile. When both are omitted, this
   * switches the request into standalone custom emulation mode.
   */
  emulation?: CustomEmulationOptions;
  /**
   * HTTP method to use for the request.
   * @default 'GET'
   */
  method?: string;
  /**
   * Additional headers to send with the request.
   * Browser-specific headers will be automatically added based on the selected browser profile.
   */
  headers?: Record<string, string> | HeaderTuple[];
  /**
   * Request body data (for POST, PUT, PATCH requests).
   */
  body?: BodyInit | null;
  /**
   * Transport instance to use for this request. When provided, transport-level
   * options such as `browser`, `os`, `proxy`, and `insecure` must not be set.
   */
  transport?: Transport;
  /**
   * Proxy URL to route the request through (e.g., 'http://proxy.example.com:8080').
   * Proxy support depends on the native layer and proxy scheme.
   */
  proxy?: string;
  /**
   * Redirect policy applied to this request. Matches the `redirect` option accepted by {@link fetch}.
   * @default "follow"
   */
  redirect?: "follow" | "manual" | "error";
  /**
   * Request timeout in milliseconds. If the request takes longer than this value,
   * it will be aborted.
   * @default 30000
   */
  timeout?: number;
  /**
   * Signal used to abort the request.
   */
  signal?: AbortSignal | null;
  /**
   * Session instance to bind this request to.
   */
  session?: Session;
  /**
   * Controls cookie scoping behavior for this request.
   */
  cookieMode?: CookieMode;
  /**
   * Identifier for the session that should handle this request.
   * @internal
   */
  sessionId?: string;
  /**
   * Internal flag indicating whether the session should be discarded once the
   * request finishes.
   * @internal
   */
  ephemeral?: boolean;
  /**
   * Disable default headers from browser emulation. When enabled, only explicitly
   * provided headers will be sent with the request, preventing emulation headers
   * from being automatically added or appended.
   * @default false
   */
  disableDefaultHeaders?: boolean;
  /**
   * Disable HTTPS certificate verification. When enabled, self-signed and invalid
   * certificates will be accepted.
   *
   * # Warning
   *
   * You should think very carefully before using this method. If invalid
   * certificates are trusted, *any* certificate for *any* site will be
   * trusted for use. This includes expired certificates. This introduces
   * significant vulnerabilities, and should only be used as a last resort.
   *
   * @default false
   */
  insecure?: boolean;
  /**
   * Controls which certificate roots are used for TLS verification.
   * @default "combined"
   */
  trustStore?: TrustStoreMode;
  /**
   * Optional callback for structured request lifecycle events emitted by the
   * native bridge.
   */
  onRequestEvent?: (event: RequestEvent) => void;
  /**
   * Capture a final diagnostics payload where supported by the native layer.
   */
  captureDiagnostics?: boolean;
}
/**
 * Internal response payload returned from the native Rust binding.
 * This interface represents the raw response data before it's converted
 * to a standard Response object.
 *
 * @internal
 */
interface NativeResponse {
  /**
   * HTTP status code (e.g., 200, 404, 500).
   */
  status: number;
  /**
   * Response headers as [name, value] tuples.
   * Header names are normalized to lowercase.
   */
  headers: HeaderTuple[];
  /**
   * Handle for streaming response body chunks from the native layer.
   * When `null`, the response does not have a body (e.g., HEAD/204/304).
   */
  bodyHandle: number | null;
  /**
   * Inline body buffer returned for small payloads. When present, `bodyHandle`
   * will be `null` to avoid a second native round-trip to read the body.
   */
  bodyBytes: Buffer | null;
  /**
   * Optional Content-Length hint reported by the server after decompression.
   */
  contentLength: number | null;
  /**
   * Cookies set by the server as [name, value] tuples.
   */
  cookies: HeaderTuple[];
  /**
   * Final URL after following any redirects.
   * If no redirects occurred, this will match the original request URL.
   */
  url: string;
  /**
   * Optional diagnostics payload collected by the native layer.
   */
  diagnostics?: RequestDiagnostics | null;
}
/**
 * Configuration options for creating a WebSocket connection.
 * Supports browser impersonation and proxies, similar to HTTP requests.
 *
 * @example
 * ```typescript
 * const ws = await websocket('wss://example.com/socket', {
 *   browser: 'chrome_142',
 *   headers: { 'Authorization': 'Bearer token' },
 * });
 *
 * ws.onmessage = (event) => {
 *   console.log('Received:', event.data);
 * };
 * ```
 */
interface WebSocketOptions {
  /**
   * Browser profile to impersonate for the WebSocket upgrade request.
   * Automatically applies browser-specific headers and TLS fingerprints.
   *
   * Accepts a concrete profile ('firefox_149') or a family alias ('firefox'),
   * which resolves to the newest profile in that family.
   * @default 'chrome'
   */
  browser?: BrowserProfile | BrowserAlias;
  /**
   * Operating system to emulate for the WebSocket handshake.
   * @default 'macos'
   */
  os?: EmulationOS;
  /**
   * Custom emulation overrides. When `browser` or `os` is present, these fields
   * layer on top of the resolved preset profile. When both are omitted, this
   * switches the handshake into standalone custom emulation mode.
   */
  emulation?: CustomEmulationOptions;
  /**
   * Additional headers to send with the WebSocket upgrade request.
   * Common headers include Authorization, Origin, or custom application headers.
   */
  headers?: HeadersInit;
  /**
   * Proxy URL to route the connection through (e.g., 'http://proxy.example.com:8080').
   * Proxy support depends on the native layer and proxy scheme.
   */
  proxy?: string;
  /**
   * Optional subprotocols for compatibility with the WHATWG WebSocket constructor.
   * Values are validated for non-empty, unique entries and sent in
   * the `Sec-WebSocket-Protocol` handshake header.
   */
  protocols?: string | string[];
  /**
   * Maximum size in bytes for a single incoming WebSocket frame.
   * Increase this when the peer sends large unfragmented frames.
   */
  maxFrameSize?: number;
  /**
   * Maximum size in bytes for a complete incoming WebSocket message.
   * Increase this when the peer sends very large fragmented messages.
   */
  maxMessageSize?: number;
  /**
   * Controls the binary payload type exposed via `MessageEvent.data`.
   * - "nodebuffer": delivers Node.js Buffer instances (default)
   * - "arraybuffer": delivers ArrayBuffer instances
   * - "blob": delivers Blob instances
   */
  binaryType?: WebSocketBinaryType;
}
interface LegacyWebSocketOptions extends WebSocketOptions {
  /**
   * @deprecated Use `websocket(url, options)` or `new WebSocket(...)`.
   */
  url: string;
  /**
   * @deprecated Use `onmessage` or `addEventListener("message", ...)`.
   */
  onMessage?: (data: string | Buffer) => void;
  /**
   * @deprecated Use `onclose` or `addEventListener("close", ...)`.
   */
  onClose?: (event: WebSocketCloseEvent) => void;
  /**
   * @deprecated Use `onerror` or `addEventListener("error", ...)`.
   */
  onError?: (error: string) => void;
}
type SessionWebSocketOptions = Omit<WebSocketOptions, "browser" | "os" | "proxy" | "emulation">;
interface LegacySessionWebSocketOptions extends SessionWebSocketOptions {
  /**
   * @deprecated Use `session.websocket(url, options)`.
   */
  url: string;
  /**
   * @deprecated Use `onmessage` or `addEventListener("message", ...)`.
   */
  onMessage?: (data: string | Buffer) => void;
  /**
   * @deprecated Use `onclose` or `addEventListener("close", ...)`.
   */
  onClose?: (event: WebSocketCloseEvent) => void;
  /**
   * @deprecated Use `onerror` or `addEventListener("error", ...)`.
   */
  onError?: (error: string) => void;
}
/**
 * Internal WebSocket connection object returned from the native Rust binding.
 * This interface contains the connection ID used to reference the WebSocket
 * in subsequent operations like sending messages or closing the connection.
 *
 * @internal
 */
interface NativeWebSocketConnection {
  /**
   * Unique identifier for this WebSocket connection.
   * Used internally to track and manage the connection.
   * @internal
   */
  _id: number;
  /**
   * Selected subprotocol returned by the server, when present.
   * @internal
   */
  protocol?: string;
  /**
   * Negotiated extension string returned by the server, when present.
   * @internal
   */
  extensions?: string;
}
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
declare class RequestError extends TypeError {
  constructor(message: string);
}
//#endregion
//#region src/wreq-js.d.ts
interface NativeWebSocketCloseEvent {
  code: number;
  reason: string;
}
type SessionDefaults = {
  transportMode: ResolvedEmulationMode;
  proxy?: string;
  timeout?: number;
  insecure?: boolean;
  trustStore?: TrustStoreMode;
  defaultHeaders?: HeaderTuple[];
  captureDiagnostics?: boolean;
  transportId?: string;
  ownsTransport?: boolean;
};
type PresetEmulationMode = {
  kind: "preset";
  browser: BrowserProfile;
  os: EmulationOS;
  emulationJson?: string;
};
type CustomEmulationMode = {
  kind: "custom";
  emulationJson: string;
};
type ResolvedEmulationMode = PresetEmulationMode | CustomEmulationMode;
type LegacyWebSocketCallbacks = {
  onMessage?: (data: string | Buffer) => void;
  onClose?: (event: WebSocketCloseEvent) => void;
  onError?: (error: string) => void;
};
type WebSocketOpenDispatchMode = "automatic" | "deferred";
type InternalWebSocketInit = {
  readonly _internal: true;
  url: string;
  options: WebSocketOptions;
  openDispatchMode: WebSocketOpenDispatchMode;
  connect: (callbacks: {
    onMessage: (data: string | Buffer) => void;
    onClose: (event: NativeWebSocketCloseEvent) => void;
    onError: (message: string) => void;
  }) => Promise<NativeWebSocketConnection>;
  legacyCallbacks: LegacyWebSocketCallbacks | undefined;
};
declare class Headers implements Iterable<[string, string]> {
  private readonly store;
  constructor(init?: HeadersInit);
  private applyInit;
  private normalizeName;
  private assertValue;
  append(name: string, value: unknown): void;
  set(name: string, value: unknown): void;
  get(name: string): string | null;
  getSetCookie(): string[];
  has(name: string): boolean;
  delete(name: string): void;
  entries(): IterableIterator<[string, string]>;
  keys(): IterableIterator<string>;
  values(): IterableIterator<string>;
  forEach(callback: (value: string, name: string, parent: Headers) => void, thisArg?: unknown): void;
  [Symbol.iterator](): IterableIterator<[string, string]>;
  toObject(): Record<string, string>;
  toTuples(): HeaderTuple[];
}
type ResponseType = "basic" | "cors" | "error" | "opaque" | "opaqueredirect";
declare class Response {
  readonly status: number;
  readonly ok: boolean;
  readonly contentLength: number | null;
  readonly url: string;
  readonly diagnostics: RequestDiagnostics | null;
  readonly type: ResponseType;
  bodyUsed: boolean;
  private readonly payload;
  private readonly requestUrl;
  private redirectedMemo;
  private readonly headersInit;
  private headersInstance;
  private readonly cookiesInit;
  private cookiesRecord;
  private inlineBody;
  private bodySource;
  private bodyStream;
  private nativeHandleAvailable;
  private nativeHandle;
  constructor(payload: NativeResponse, requestUrl: string, bodySource?: ReadableStream$1<Uint8Array> | null);
  get redirected(): boolean;
  get statusText(): string;
  get headers(): Headers;
  get cookies(): Record<string, string | string[]>;
  get body(): ReadableStream$1<Uint8Array> | null;
  json<T = unknown>(): Promise<T>;
  arrayBuffer(): Promise<ArrayBuffer>;
  bytes(): Promise<Uint8Array>;
  text(): Promise<string>;
  blob(): Promise<Blob>;
  formData(): Promise<FormData>;
  readable(): Readable;
  clone(): Response;
  private assertBodyAvailable;
  private consumeBody;
}
declare class Transport {
  readonly id: string;
  private disposed;
  constructor(id: string);
  get closed(): boolean;
  close(): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>;
}
declare class Session implements SessionHandle {
  readonly id: string;
  private disposed;
  private readonly defaults;
  constructor(id: string, defaults: SessionDefaults);
  get closed(): boolean;
  private ensureActive;
  /** @internal */
  getDefaults(): SessionDefaults;
  /** @internal */
  _defaultsRef(): SessionDefaults;
  fetch(input: string | URL | Request, init?: RequestInit): Promise<Response>;
  clearCookies(): Promise<void>;
  getCookies(url: string | URL): Record<string, string>;
  getAllCookies(): SessionCookie[];
  setCookie(name: string, value: string, url: string | URL): void;
  /**
   * Create a WebSocket connection that shares this session's cookies and TLS configuration.
   *
   * @param urlOrOptions - WebSocket URL or legacy options object
   * @param options - Session WebSocket options
   * @returns Promise that resolves to the WebSocket instance
   */
  websocket(url: string | URL, options?: SessionWebSocketOptions): Promise<WebSocket>;
  websocket(options: LegacySessionWebSocketOptions): Promise<WebSocket>;
  close(): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>;
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
declare function fetch(input: string | URL | Request, init?: RequestInit): Promise<Response>;
declare function createTransport(options?: CreateTransportOptions): Promise<Transport>;
declare function createSession(options?: CreateSessionOptions): Promise<Session>;
declare function withSession<T>(fn: (session: Session) => Promise<T> | T, options?: CreateSessionOptions): Promise<T>;
/**
 * @deprecated Use {@link fetch} instead.
 */
declare function request(options: RequestOptions): Promise<Response>;
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
declare function getProfiles(): BrowserProfile[];
/**
 * Get list of supported operating systems for emulation.
 *
 * @returns Array of operating system identifiers
 */
declare function getOperatingSystems(): EmulationOS[];
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
declare function resolveProfile(browser: BrowserProfile | BrowserAlias): BrowserProfile;
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
declare function getEmulationHeaders(browser?: BrowserProfile | BrowserAlias, os?: EmulationOS): Headers;
/**
 * Convenience helper for GET requests using {@link fetch}.
 */
declare function get(url: string | URL | Request, init?: Omit<RequestInit, "method">): Promise<Response>;
/**
 * Convenience helper for POST requests using {@link fetch}.
 */
declare function post(url: string | URL | Request, body?: BodyInit | null, init?: Omit<RequestInit, "method" | "body">): Promise<Response>;
type WebSocketAddEventListenerOptions = boolean | {
  once?: boolean;
  signal?: AbortSignal | null;
};
/**
 * WHATWG-style WebSocket API with async connection establishment.
 */
declare class WebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;
  readonly url: string;
  protocol: string;
  extensions: string;
  readyState: number;
  private _binaryType;
  private _bufferedAmount;
  private _onopen;
  private _onmessage;
  private _onclose;
  private _onerror;
  private _onHandlerOrder;
  private _listenerOrderCounter;
  private readonly _listeners;
  private readonly _legacyCallbacks;
  private readonly _openDispatchMode;
  private _connection;
  private _connectPromise;
  private _closeOptions;
  private _finalizerToken;
  private _openEventDispatched;
  private _openEventQueued;
  private _closeEventDispatched;
  private _nativeCloseStarted;
  private _pendingMessages;
  private _sendChain;
  constructor(init: InternalWebSocketInit);
  constructor(url: string | URL, protocols?: string | string[]);
  constructor(url: string | URL, protocols?: string | string[], options?: WebSocketOptions);
  constructor(url: string | URL, options?: WebSocketOptions);
  get binaryType(): WebSocketBinaryType;
  set binaryType(value: WebSocketBinaryType);
  get bufferedAmount(): number;
  get onopen(): ((this: WebSocket, event: WebSocketOpenEvent) => void) | null;
  set onopen(listener: ((this: WebSocket, event: WebSocketOpenEvent) => void) | null);
  get onmessage(): ((this: WebSocket, event: WebSocketMessageEvent) => void) | null;
  set onmessage(listener: ((this: WebSocket, event: WebSocketMessageEvent) => void) | null);
  get onclose(): ((this: WebSocket, event: WebSocketCloseEvent) => void) | null;
  set onclose(listener: ((this: WebSocket, event: WebSocketCloseEvent) => void) | null);
  get onerror(): ((this: WebSocket, event: WebSocketErrorEvent) => void) | null;
  set onerror(listener: ((this: WebSocket, event: WebSocketErrorEvent) => void) | null);
  static _connectWithInit(init: InternalWebSocketInit): Promise<WebSocket>;
  private static buildStandaloneInit;
  private connect;
  private _waitUntilConnected;
  private scheduleOpenEventAfterConnect;
  private scheduleOpenEventAfterAwait;
  private scheduleOpenEventWithDepth;
  private releaseConnectionTracking;
  private toMessageEventData;
  private invokeListener;
  private createBaseEvent;
  private getOnHandler;
  private getOnHandlerOrder;
  private getListenerMap;
  private dispatchEvent;
  private dispatchOpenEvent;
  private dispatchMessageEvent;
  private dispatchCloseEvent;
  private dispatchErrorEvent;
  private handleNativeMessage;
  private handleNativeError;
  private handleNativeClose;
  private finalizeClosed;
  private startNativeClose;
  addEventListener(type: "open", listener: ((event: WebSocketOpenEvent) => void) | null, options?: WebSocketAddEventListenerOptions): void;
  addEventListener(type: "message", listener: ((event: WebSocketMessageEvent) => void) | null, options?: WebSocketAddEventListenerOptions): void;
  addEventListener(type: "close", listener: ((event: WebSocketCloseEvent) => void) | null, options?: WebSocketAddEventListenerOptions): void;
  addEventListener(type: "error", listener: ((event: WebSocketErrorEvent) => void) | null, options?: WebSocketAddEventListenerOptions): void;
  addEventListener(type: string, listener: unknown, options?: WebSocketAddEventListenerOptions): void;
  removeEventListener(type: "open", listener: ((event: WebSocketOpenEvent) => void) | null): void;
  removeEventListener(type: "message", listener: ((event: WebSocketMessageEvent) => void) | null): void;
  removeEventListener(type: "close", listener: ((event: WebSocketCloseEvent) => void) | null): void;
  removeEventListener(type: "error", listener: ((event: WebSocketErrorEvent) => void) | null): void;
  removeEventListener(type: string, listener: unknown): void;
  private getSendByteLength;
  private normalizeSendPayload;
  send(data: string | Buffer | ArrayBuffer | ArrayBufferView | Blob): void;
  close(code?: number, reason?: string): void;
  [Symbol.dispose](): void;
}
/**
 * Create a WebSocket connection with browser impersonation.
 */
declare function websocket(url: string | URL, options?: WebSocketOptions): Promise<WebSocket>;
declare function websocket(options: LegacyWebSocketOptions): Promise<WebSocket>;
declare const _default: {
  fetch: typeof fetch;
  request: typeof request;
  get: typeof get;
  post: typeof post;
  getProfiles: typeof getProfiles;
  getOperatingSystems: typeof getOperatingSystems;
  createTransport: typeof createTransport;
  createSession: typeof createSession;
  withSession: typeof withSession;
  websocket: typeof websocket;
  WebSocket: typeof WebSocket;
  Headers: typeof Headers;
  Response: typeof Response;
  Transport: typeof Transport;
  Session: typeof Session;
  RequestError: typeof RequestError;
};
//#endregion
export { type AlpnProtocol, type AlpsProtocol, type BodyInit, type BrowserAlias, type BrowserProfile, type CookieMode, type CreateSessionOptions, type CreateTransportOptions, type CustomEmulationOptions, type CustomHttp1Options, type CustomHttp2Options, type CustomTlsOptions, type EmulationOS, Headers, type HeadersInit, type Http2Priority, type Http2PseudoHeaderId, type Http2SettingId, type Http2StreamDependency, type RequestDiagnostics, RequestError, type RequestEvent, type RequestEventType, type RequestInit, type RequestOptions, Response, Session, type SessionHandle, type SessionWebSocketOptions, type TlsVersion, Transport, WebSocket, type WebSocketBinaryType, type WebSocketCloseEvent, type WebSocketErrorEvent, type WebSocketEventType, type WebSocketMessageEvent, type WebSocketOpenEvent, type WebSocketOptions, createSession, createTransport, _default as default, fetch, get, getEmulationHeaders, getOperatingSystems, getProfiles, post, request, resolveProfile, websocket, withSession };
//# sourceMappingURL=wreq-js.d.ts.map