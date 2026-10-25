// Type definitions for the ani.pm Player SDK v1 · https://ani.pm/developers
// Usage with a bundler: declare the global, e.g. `/// <reference path="./anipm-player.d.ts" />`, and load
// https://ani.pm/embed/sdk/v1.js with a <script> tag (the SDK is also CommonJS-compatible).

export type Language = 'sub' | 'dub'
export type PlayerStatus = 'idle' | 'loading' | 'ready' | 'error'

export interface PlayerOptions {
  /** Optional. Without a key the embed is open (any site, no signup). An app's publishable key (pk_live_…) adds usage
   *  statistics, the app's defaults and, if the app lists sites, a lock to them. */
  key?: string
  /** AniList id (154587) or 'mal-<MyAnimeList id>'. */
  anime: number | string
  /** Episode number, 1-based. Default 1. */
  episode?: number
  lang?: Language
  /** Prefer the burned-in subtitle version where one exists. */
  hardsub?: boolean
  /** Start position in seconds. */
  start?: number
  /** Start playing as soon as the player loads. Default true. false: the title poster waits for the viewer to press
   *  Play. Browsers that block sound start it muted (combine with muted to always start silent). */
  autoplay?: boolean
  muted?: boolean
  /** With autoplay false, show the title poster until the viewer presses Play. Default true. false: start at once
   *  anyway. */
  poster?: boolean
  /** Play the next episode when one ends. The viewer's own choice is remembered; a value given here wins for this load. */
  autoNext?: boolean
  /** Skip intros and outros. Remembered like autoNext. */
  autoSkip?: boolean
  /** Start in theater mode. Remembered like autoNext. */
  theater?: boolean
  /** Default true: in theater mode the SDK widens the frame to the window's width (up to 85% of its height). Set false to
   *  lay it out yourself: the element you mounted in carries data-anipm-theater while theater is on. */
  theaterLayout?: boolean
  /** Default true: keep a copy of the viewer's player settings in your page's storage, so they survive browsers that
   *  clear a framed site's storage (Safari). */
  rememberSettings?: boolean
  /** Default true: tell your app's analytics which page of your site the player is on (the path only, never a query
   *  string or fragment). Set false to keep pages out of your analytics. */
  reportPage?: boolean
  /** Show the in-player episode browser. Default true. */
  episodes?: boolean
  /** Accent color, #rrggbb. */
  color?: string
  /** 'off', or a subtitle label to select (e.g. 'English'). */
  subtitles?: string
  /** Accessible iframe title. */
  title?: string
  width?: string
  height?: string | number
  /** CSS aspect-ratio when no height is given. Default '16 / 9'. */
  aspectRatio?: string
  lazy?: boolean
  /** Set false to disable the postMessage API (plain player). */
  api?: boolean
  /** 18+ titles play by default; false hides them in an open embed (an app decides this in its settings). */
  adult?: boolean
  on?: Partial<{ [K in PlayerEventName]: (data: PlayerEventMap[K], player: Player) => void }>
}

export interface Quality { height: number; bitrate: number | null }
export interface SubtitleTrack { label: string; language: string; showing: boolean }
export interface Availability { sub: boolean; dub: boolean; subHard: boolean; dubHard: boolean }

export interface PlayerState {
  status: PlayerStatus
  anilistId: number | null
  malId: number | null
  title: string
  episode: number
  lang: Language
  hardsub: boolean
  currentTime: number
  duration: number
  paused: boolean
  ended: boolean
  buffering: boolean
  volume: number
  muted: boolean
  playbackRate: number
  fullscreen: boolean
  autoNext: boolean
  autoSkip: boolean
  /** Theater mode: the player's big-canvas presentation (pause menu, ambient glow). */
  theater: boolean
  /** The ambient glow on this device; null until the player has said. */
  ambient: boolean | null
  quality: { height: number | null; auto: boolean } | null
  qualities: Quality[]
  subtitles: { enabled: boolean | null; label: string | null }
  subtitleTracks: SubtitleTrack[]
  availability: Availability | null
  hasPrevious: boolean
  hasNext: boolean
  /** True while an autoplay: false player waits on its title poster for the viewer to press Play. */
  awaitingPress?: boolean
}

export interface Episode {
  number: number
  title: string | null
  runtimeSeconds: number | null
  aired: string | null
  thumbnail: string | null
  available: Availability
}

export interface PlayerEventMap {
  loaded: PlayerState
  ready: PlayerState
  play: PlayerState
  pause: PlayerState
  playing: PlayerState
  waiting: PlayerState
  seeking: PlayerState
  seeked: PlayerState
  timeupdate: { currentTime: number; duration: number; paused: boolean }
  durationchange: PlayerState
  ended: PlayerState
  error: { code: string; message: string }
  episodechange: PlayerState
  languagechange: PlayerState
  episodes: { episodes: Episode[] }
  volumechange: { volume: number; muted: boolean } | PlayerState
  ratechange: PlayerState
  qualitychange: { quality: PlayerState['quality']; qualities: Quality[] }
  subtitlechange: { subtitles: PlayerState['subtitles']; tracks: SubtitleTrack[] }
  fullscreenchange: { fullscreen: boolean }
  autonextchange: { autoNext: boolean }
  autoskipchange: { autoSkip: boolean }
  autoplayblocked: PlayerState
  theaterchange: { theater: boolean }
  ambientchange: { ambient: boolean }
  /** The viewer's saved settings changed (the SDK keeps this copy for you unless rememberSettings is false). */
  settings: { settings: unknown }
}
export type PlayerEventName = keyof PlayerEventMap

export declare class AniPMError extends Error {
  readonly code: string
}

export declare class Player {
  constructor(target: string | Element, options: PlayerOptions)
  static readonly version: string
  readonly iframe: HTMLIFrameElement
  /** Last known state, updated from events. */
  readonly state: PlayerState
  on<K extends PlayerEventName>(event: K, listener: (data: PlayerEventMap[K], player: Player) => void): () => void
  on(event: '*', listener: (event: PlayerEventName, data: unknown, player: Player) => void): () => void
  off(event: PlayerEventName | '*', listener: (...args: any[]) => void): this
  once<K extends PlayerEventName>(event: K, listener: (data: PlayerEventMap[K], player: Player) => void): () => void
  /** Resolves when the embed answers (not when video plays; see the 'ready' event). */
  ready(): Promise<PlayerState>
  /** Starts playback as if Play was pressed (an autoplay: false player waits for this or for the viewer). */
  start(): Promise<PlayerState>
  /** Plays; starts the player first if it has not started yet. */
  play(): Promise<PlayerState>
  pause(): Promise<PlayerState>
  togglePlay(): Promise<PlayerState>
  seek(seconds: number): Promise<PlayerState>
  seekBy(deltaSeconds: number): Promise<PlayerState>
  setVolume(volume: number): Promise<PlayerState>
  setMuted(muted: boolean): Promise<PlayerState>
  mute(): Promise<PlayerState>
  unmute(): Promise<PlayerState>
  setPlaybackRate(rate: number): Promise<PlayerState>
  setSubtitles(choice: false | 'off' | string | { label?: string; language?: string }): Promise<PlayerState>
  /** A picture height from state.qualities. There is no automatic quality. */
  setQuality(height: number): Promise<PlayerState>
  setLanguage(lang: Language): Promise<PlayerState>
  setHardsub(on: boolean): Promise<PlayerState>
  loadEpisode(episode: number, options?: { lang?: Language; start?: number }): Promise<PlayerState>
  next(): Promise<PlayerState>
  previous(): Promise<PlayerState>
  setAutoNext(on: boolean): Promise<PlayerState>
  setAutoSkip(on: boolean): Promise<PlayerState>
  setTheater(on: boolean): Promise<PlayerState>
  toggleTheater(): Promise<PlayerState>
  /** The ambient glow (shows in theater and fullscreen). Remembered by the player on this device. */
  setAmbient(on: boolean): Promise<PlayerState>
  setAccent(color: string): Promise<PlayerState>
  getState(): Promise<PlayerState>
  getEpisodes(): Promise<Episode[]>
  loadTitle(anime: number | string, episode?: number, options?: Partial<PlayerOptions>): Promise<PlayerState>
  /** Call from your own click handler. */
  requestFullscreen(): Promise<void>
  exitFullscreen(): Promise<void>
  destroy(): void
}

export declare function embedUrl(options: PlayerOptions): string
export declare function mountAll(scope?: ParentNode): Player[]
export declare const version: string
export declare const events: PlayerEventName[]

declare global {
  interface Window {
    AniPM: { Player: typeof Player; embedUrl: typeof embedUrl; mountAll: typeof mountAll; AniPMError: typeof AniPMError; version: string; events: PlayerEventName[] }
  }
}
