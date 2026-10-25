/*! ani.pm Player SDK v1.4.0 · https://ani.pm/developers
 *
 *   <div id="player"></div>
 *   <script src="https://ani.pm/embed/sdk/v1.js"></script>
 *   <script>
 *     const player = new AniPM.Player('#player', { anime: 154587, episode: 1 })   // no key needed
 *     player.on('ready', (state) => console.log('playing', state.title, state.episode))
 *   </script>
 *
 * Markup only:  <div data-anipm-player data-anime="154587" data-episode="1"></div>
 *
 * Every method returns a Promise that settles with the player's state (or rejects with an Error whose `code` says
 * why). Messages are { ns: 'anipm.player', v: 1 } envelopes, exchanged only with the embed's own frame and origin.
 */
;(function (global, factory) {
  const api = factory()
  if (typeof module === 'object' && module && typeof module.exports === 'object') module.exports = api
  if (global) global.AniPM = Object.assign(global.AniPM || {}, api)
})(typeof window !== 'undefined' ? window : undefined, function () {
  'use strict'

  const VERSION = '1.4.1'
  const NS = 'anipm.player'
  const API = 1
  const DEFAULT_ORIGIN = 'https://ani.pm'
  const COMMAND_TIMEOUT_MS = 15000
  const EVENTS = ['loaded', 'ready', 'play', 'pause', 'playing', 'waiting', 'seeking', 'seeked', 'timeupdate', 'durationchange',
    'ended', 'error', 'episodechange', 'languagechange', 'episodes', 'volumechange', 'ratechange', 'qualitychange',
    'subtitlechange', 'fullscreenchange', 'autonextchange', 'autoskipchange', 'autoplayblocked', 'theaterchange',
    'ambientchange', 'settings']
  // The viewer's player settings, kept in YOUR page's storage too: some browsers clear a framed site's storage when the
  // tab closes, and the embed takes this copy back when its own is gone (hello { settings }).
  const SETTINGS_KEY = 'anipm:player-settings:v1'
  const readSettings = () => { try { return JSON.parse(window.localStorage.getItem(SETTINGS_KEY) || 'null') || undefined } catch { return undefined } }
  const writeSettings = (value) => { try { window.localStorage.setItem(SETTINGS_KEY, JSON.stringify(value)) } catch { /* storage refused */ } }

  class AniPMError extends Error {
    constructor(code, message) { super(message || code); this.name = 'AniPMError'; this.code = code }
  }

  const bool = (value) => (value === undefined || value === null ? undefined : value === true || value === 'true' || value === '1' || value === 1)

  /**
   * The embed URL for a plain <iframe>: every option the Player constructor takes. No key needed: without one the URL
   * is the open form, https://ani.pm/embed/ani/<anilistId>/<episode>/<sub|dub> (or /embed/mal/<malId>/…).
   */
  function embedUrl(options) {
    const o = options || {}
    if (o.key != null && o.key !== '' && !/^pk_live_[0-9A-Za-z]{24}$/.test(String(o.key))) throw new AniPMError('invalid-key', 'options.key, when given, must be your publishable key (pk_live_…). Leave it out for an open embed.')
    const anime = String(o.anime == null ? '' : o.anime).trim()
    if (!/^(?:[1-9]\d{0,9}|mal-[1-9]\d{0,9})$/.test(anime)) throw new AniPMError('invalid-anime', "options.anime must be an AniList id (154587) or 'mal-<id>'.")
    const episode = o.episode == null ? 1 : Number(o.episode)
    if (!Number.isSafeInteger(episode) || episode < 1) throw new AniPMError('invalid-episode', 'options.episode must be a positive whole number.')
    const origin = String(o.origin || DEFAULT_ORIGIN).replace(/\/+$/, '')
    const lang = o.lang === 'dub' ? 'dub' : 'sub'
    const query = new URLSearchParams()
    let path
    if (o.key) {
      path = `/embed/v1/${anime}/${episode}`
      query.set('key', o.key)
      if (o.lang === 'dub' || o.lang === 'sub') query.set('lang', o.lang)
    } else {
      path = anime.startsWith('mal-') ? `/embed/mal/${anime.slice(4)}/${episode}/${lang}` : `/embed/ani/${anime}/${episode}/${lang}`
    }
    if (o.adult === false || o.adult === 'false' || o.adult === '0' || o.adult === 0) query.set('adult', '0')
    if (bool(o.hardsub) !== undefined) query.set('hardsub', bool(o.hardsub) ? '1' : '0')
    if (Number(o.start) > 0) query.set('start', String(Math.floor(Number(o.start))))
    if (bool(o.autoplay) !== undefined) query.set('autoplay', bool(o.autoplay) ? '1' : '0')
    if (bool(o.muted) !== undefined) query.set('muted', bool(o.muted) ? '1' : '0')
    if (bool(o.poster) !== undefined) query.set('poster', bool(o.poster) ? '1' : '0')
    if (bool(o.autoNext) !== undefined) query.set('autonext', bool(o.autoNext) ? '1' : '0')
    if (bool(o.autoSkip) !== undefined) query.set('autoskip', bool(o.autoSkip) ? '1' : '0')
    if (bool(o.theater) !== undefined) query.set('theater', bool(o.theater) ? '1' : '0')
    if (bool(o.episodes) !== undefined) query.set('episodes', bool(o.episodes) ? '1' : '0')
    if (typeof o.color === 'string' && /^#?[0-9a-fA-F]{6}$/.test(o.color)) query.set('color', o.color.replace('#', ''))
    if (typeof o.subtitles === 'string' && o.subtitles) query.set('subtitles', o.subtitles)
    if (o.api === false) query.set('api', '0')
    const search = query.toString()
    return `${origin}${path}${search ? `?${search}` : ''}`
  }

  class Player {
    constructor(target, options) {
      const host = typeof target === 'string' ? document.querySelector(target) : target
      if (!host || typeof host.appendChild !== 'function') throw new AniPMError('invalid-target', 'Player target must be an element or a selector that finds one.')
      this._options = Object.assign({}, options || {})
      this._origin = String(this._options.origin || DEFAULT_ORIGIN).replace(/\/+$/, '')
      this._listeners = new Map()
      this._pending = new Map()
      this._queue = []
      this._nextId = 1
      this._connected = false
      this._destroyed = false
      this.state = {
        status: 'idle', anilistId: null, episode: Number(this._options.episode) || 1, lang: this._options.lang === 'dub' ? 'dub' : 'sub',
        currentTime: 0, duration: 0, paused: true, ended: false, volume: 1, muted: !!this._options.muted, playbackRate: 1,
        theater: false, ambient: null,
      }
      let readyResolve
      this._readyPromise = new Promise((resolve) => { readyResolve = resolve })
      this._readyResolve = readyResolve
      this._host = host
      this._theaterShown = false
      this._theaterFit = null

      const iframe = document.createElement('iframe')
      iframe.src = embedUrl(this._options)
      iframe.title = this._options.title || 'Video player'
      iframe.allow = 'autoplay; fullscreen; picture-in-picture; encrypted-media; clipboard-write; screen-wake-lock'
      iframe.setAttribute('allowfullscreen', '')
      iframe.referrerPolicy = 'strict-origin-when-cross-origin'
      if (this._options.lazy) iframe.loading = 'lazy'
      iframe.style.border = '0'
      iframe.style.display = 'block'
      this.iframe = iframe
      this._baseSize()

      this._onMessage = (event) => this._receive(event)
      window.addEventListener('message', this._onMessage)
      iframe.addEventListener('load', () => this._handshake())
      host.appendChild(iframe)
      for (const [name, fn] of Object.entries(this._options.on || {})) this.on(name, fn)
    }

    static get version() { return VERSION }

    // The iframe's size from the options (16:9 at the element's width unless told otherwise).
    _baseSize() {
      const style = this.iframe.style
      style.width = this._options.width || '100%'
      style.maxWidth = ''
      style.marginLeft = ''
      if (this._options.height) {
        style.height = typeof this._options.height === 'number' ? `${this._options.height}px` : String(this._options.height)
        style.aspectRatio = ''
      } else {
        style.height = ''
        style.aspectRatio = this._options.aspectRatio || '16 / 9'
      }
    }

    // THEATER, the SDK's side: the player's theater button (or setTheater) turns it on, the element gets
    // data-anipm-theater, and unless options.theaterLayout is false the frame spans the window's width at up to 85% of
    // its height - wider than the picture on most screens, which is where the player's ambient glow shows.
    _syncTheater() {
      const on = !!this.state.theater
      if (!this.iframe || on === this._theaterShown) return
      this._theaterShown = on
      if (on) this._host.setAttribute('data-anipm-theater', '')
      else this._host.removeAttribute('data-anipm-theater')
      if (bool(this._options.theaterLayout) === false) return
      if (on) {
        let frame = 0
        this._theaterFit = () => {
          cancelAnimationFrame(frame)
          frame = requestAnimationFrame(() => {
            const el = this.iframe
            if (!el || !this._theaterShown) return
            const width = document.documentElement.clientWidth
            el.style.marginLeft = '0px'
            const left = el.getBoundingClientRect().left
            el.style.width = `${width}px`
            el.style.maxWidth = 'none'
            el.style.marginLeft = `${-Math.round(left)}px`
            el.style.aspectRatio = 'auto'
            el.style.height = `${Math.round(Math.min((width * 9) / 16, window.innerHeight * 0.85))}px`
          })
        }
        window.addEventListener('resize', this._theaterFit)
        this._theaterFit()
      } else {
        if (this._theaterFit) window.removeEventListener('resize', this._theaterFit)
        this._theaterFit = null
        this._baseSize()
      }
    }

    _handshake() {
      let tries = 0
      const settings = bool(this._options.rememberSettings) === false ? undefined : readSettings()
      // The page's path (never its query or fragment) for your analytics' pages, unless options.reportPage is false.
      let page
      try { page = bool(this._options.reportPage) === false ? undefined : String(window.location.pathname || '/').slice(0, 160) } catch { page = undefined }
      const knock = () => {
        if (this._connected || this._destroyed || tries++ > 20) return
        this._post({ cmd: 'hello', id: 0, args: { sdk: VERSION, settings, page } })
        setTimeout(knock, 300)
      }
      knock()
    }

    _post(message) {
      const target = this.iframe && this.iframe.contentWindow
      if (!target) return
      target.postMessage(Object.assign({ ns: NS, v: API }, message), this._origin)
    }

    _receive(event) {
      if (this._destroyed || !this.iframe || event.source !== this.iframe.contentWindow || event.origin !== this._origin) return
      const message = event.data
      if (!message || typeof message !== 'object' || message.ns !== NS || message.v !== API) return
      if (message.id === 0 && message.ok) {
        if (this._connected) return
        this._connected = true
        if (message.result && message.result.state) Object.assign(this.state, message.result.state)
        this._syncTheater()
        const queued = this._queue.splice(0)
        for (const item of queued) this._post(item)
        this._readyResolve(this.state)
        return
      }
      if (message.id != null && this._pending.has(message.id)) {
        const pending = this._pending.get(message.id)
        this._pending.delete(message.id)
        clearTimeout(pending.timer)
        if (message.ok) {
          if (message.result && typeof message.result === 'object' && 'status' in message.result && 'episode' in message.result) { Object.assign(this.state, message.result); this._syncTheater() }
          pending.resolve(message.result)
        } else {
          const error = message.error || {}
          pending.reject(new AniPMError(error.code || 'command-failed', error.message))
        }
        return
      }
      if (typeof message.event === 'string') {
        const data = message.data
        if (data && typeof data === 'object') {
          if ('status' in data && 'episode' in data) Object.assign(this.state, data)
          else if (message.event === 'timeupdate') Object.assign(this.state, data)
          else if (['volumechange', 'fullscreenchange', 'autonextchange', 'autoskipchange', 'theaterchange', 'ambientchange'].includes(message.event)) Object.assign(this.state, data)
          else if (message.event === 'qualitychange') Object.assign(this.state, { quality: data.quality, qualities: data.qualities })
          else if (message.event === 'subtitlechange') Object.assign(this.state, { subtitles: data.subtitles, subtitleTracks: data.tracks })
          else if (message.event === 'settings' && data.settings && bool(this._options.rememberSettings) !== false) writeSettings(data.settings)
          this._syncTheater()
        }
        this._emit(message.event, data)
      }
    }

    _emit(name, data) {
      const set = this._listeners.get(name)
      if (set) for (const fn of Array.from(set)) { try { fn.call(this, data, this) } catch (error) { setTimeout(() => { throw error }) } }
      const any = this._listeners.get('*')
      if (any) for (const fn of Array.from(any)) { try { fn.call(this, name, data, this) } catch (error) { setTimeout(() => { throw error }) } }
    }

    /** Subscribe; returns an unsubscribe function. `'*'` receives (eventName, data). */
    on(name, fn) {
      if (typeof fn !== 'function') throw new AniPMError('invalid-listener', 'Listener must be a function.')
      if (name !== '*' && !EVENTS.includes(name)) throw new AniPMError('unknown-event', `Unknown event ${name}. Known: ${EVENTS.join(', ')}`)
      if (!this._listeners.has(name)) this._listeners.set(name, new Set())
      this._listeners.get(name).add(fn)
      return () => this.off(name, fn)
    }
    off(name, fn) { const set = this._listeners.get(name); if (set) set.delete(fn); return this }
    once(name, fn) {
      const off = this.on(name, (...args) => { off(); fn.apply(this, args) })
      return off
    }

    /** Resolves once the embed has answered (not when video plays: listen for 'ready' for that). */
    ready() { return this._readyPromise }

    _command(cmd, args) {
      if (this._destroyed) return Promise.reject(new AniPMError('destroyed', 'This player was destroyed.'))
      return new Promise((resolve, reject) => {
        const id = this._nextId++
        const timer = setTimeout(() => {
          this._pending.delete(id)
          reject(new AniPMError('timeout', `The player did not answer ${cmd} in time.`))
        }, COMMAND_TIMEOUT_MS)
        this._pending.set(id, { resolve, reject, timer })
        const message = { id, cmd, args: args || {} }
        if (this._connected) this._post(message)
        else this._queue.push(message)
      })
    }

    play() { return this._command('play') }
    pause() { return this._command('pause') }
    togglePlay() { return this._command('togglePlay') }
    seek(time) { return this._command('seek', { time }) }
    seekBy(delta) { return this._command('seek', { time: Math.max(0, (this.state.currentTime || 0) + Number(delta || 0)) }) }
    setVolume(volume) { return this._command('setVolume', { volume }) }
    setMuted(muted) { return this._command('setMuted', { muted: !!muted }) }
    mute() { return this.setMuted(true) }
    unmute() { return this.setMuted(false) }
    setPlaybackRate(rate) { return this._command('setPlaybackRate', { rate }) }
    /** false = off; { label: 'English' } or { language: 'en' } = on. */
    setSubtitles(choice) {
      if (choice === false || choice === null || choice === 'off') return this._command('setSubtitles', { enabled: false })
      if (typeof choice === 'string') return this._command('setSubtitles', { enabled: true, language: choice.length <= 3 ? choice : undefined, label: choice.length > 3 ? choice : undefined })
      return this._command('setSubtitles', Object.assign({ enabled: true }, choice || {}))
    }
    /** A picture height from state.qualities (1080, 720, …). The player has no automatic quality. */
    setQuality(height) { return this._command('setQuality', { height }) }
    setLanguage(lang) { return this._command('setLanguage', { lang }) }
    setHardsub(on) { return this._command('setHardsub', { on: !!on }) }
    loadEpisode(episode, options) { return this._command('loadEpisode', Object.assign({ episode }, options || {})) }
    next() { return this._command('next') }
    previous() { return this._command('previous') }
    setAutoNext(on) { return this._command('setAutoNext', { on: !!on }) }
    setAutoSkip(on) { return this._command('setAutoSkip', { on: !!on }) }
    /** Theater: the player's big-canvas mode (pause menu, ambient glow); the SDK widens the frame unless options.theaterLayout is false. */
    setTheater(on) { return this._command('setTheater', { on: !!on }) }
    toggleTheater() { return this._command('toggleTheater') }
    /** The ambient glow around the picture (shows in theater and fullscreen); the player remembers it on this device. */
    setAmbient(on) { return this._command('setAmbient', { on: !!on }) }
    setAccent(color) { return this._command('setAccent', { color }) }
    start() { return this._command('start') }
    getState() { return this._command('getState').then((state) => Object.assign(this.state, state)) }
    getEpisodes() { return this._command('getEpisodes') }

    /** Another title in the same element: the embed reloads with it. */
    loadTitle(anime, episode, options) {
      Object.assign(this._options, options || {}, { anime, episode: episode || 1 })
      this._connected = false
      for (const [, pending] of this._pending) { clearTimeout(pending.timer); pending.reject(new AniPMError('reloaded', 'The player loaded another title.')) }
      this._pending.clear()
      this._queue = []
      this._readyPromise = new Promise((resolve) => { this._readyResolve = resolve })
      this.iframe.src = embedUrl(this._options)
      return this._readyPromise
    }

    /** Must run inside your own click/keypress handler (browsers require a user gesture for fullscreen). */
    requestFullscreen() {
      const el = this.iframe
      const request = el.requestFullscreen || el.webkitRequestFullscreen
      if (!request) return Promise.reject(new AniPMError('fullscreen-unsupported', 'Fullscreen is not available here.'))
      return Promise.resolve(request.call(el))
    }
    exitFullscreen() {
      const exit = document.exitFullscreen || document.webkitExitFullscreen
      return exit && (document.fullscreenElement || document.webkitFullscreenElement) ? Promise.resolve(exit.call(document)) : Promise.resolve()
    }

    destroy() {
      if (this._destroyed) return
      this._destroyed = true
      window.removeEventListener('message', this._onMessage)
      if (this._theaterFit) window.removeEventListener('resize', this._theaterFit)
      this._theaterFit = null
      if (this._host) this._host.removeAttribute('data-anipm-theater')
      for (const [, pending] of this._pending) { clearTimeout(pending.timer); pending.reject(new AniPMError('destroyed', 'This player was destroyed.')) }
      this._pending.clear()
      this._listeners.clear()
      if (this.iframe) this.iframe.remove()
      this.iframe = null
    }
  }

  /** Mount every <div data-anipm-player …> on the page (runs automatically once the DOM is ready). */
  function mountAll(scope) {
    const nodes = (scope || document).querySelectorAll('[data-anipm-player]:not([data-anipm-mounted])')
    const players = []
    for (const node of Array.from(nodes)) {
      const d = node.dataset
      try {
        node.setAttribute('data-anipm-mounted', '')
        const player = new Player(node, {
          key: d.key, anime: d.anime, episode: d.episode, lang: d.lang, hardsub: d.hardsub, start: d.start, autoplay: d.autoplay,
          muted: d.muted, poster: d.poster, autoNext: d.autonext, autoSkip: d.autoskip, episodes: d.episodes, color: d.color,
          subtitles: d.subtitles, title: d.title, origin: d.origin, lazy: d.lazy !== undefined, adult: d.adult,
          theater: d.theater, theaterLayout: d.theaterLayout, rememberSettings: d.rememberSettings, reportPage: d.reportPage,
        })
        node.anipmPlayer = player
        players.push(player)
      } catch (error) {
        if (typeof console !== 'undefined') console.error('[ani.pm] could not mount player:', error && error.message)
      }
    }
    return players
  }
  if (typeof document !== 'undefined') {
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', () => mountAll(), { once: true })
    else setTimeout(() => mountAll(), 0)
  }

  return { Player, embedUrl, mountAll, AniPMError, version: VERSION, events: EVENTS.slice() }
})
