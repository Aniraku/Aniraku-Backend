package anipm

import (
	"encoding/json"
	"sync"
	"time"
)

// Envelope is one postMessage payload of the ani.pm player protocol. The SDK
// frames every message as an object { ns: 'anipm.player', v: 1, … }: commands
// carry { cmd, id, args }, answers carry { id, ok, result | error } and events
// carry { event, data }.
type Envelope struct {
	NS     string     `json:"ns"`
	V      int        `json:"v"`
	Cmd    string     `json:"cmd,omitempty"`
	ID     *int       `json:"id,omitempty"`
	Args   any        `json:"args,omitempty"`
	OK     *bool      `json:"ok,omitempty"`
	Result any        `json:"result,omitempty"`
	Error  *WireError `json:"error,omitempty"`
	Event  string     `json:"event,omitempty"`
	Data   any        `json:"data,omitempty"`
}

// WireError is the { code, message } shape of an error answer or the data of an
// "error" event.
type WireError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func intPtr(v int) *int    { return &v }
func boolPtr(v bool) *bool { return &v }

// joinEvents renders the EVENTS list the way the SDK's unknown-event message
// does: EVENTS.join(', ').
func joinEvents() string {
	out := ""
	for i, e := range Events {
		if i > 0 {
			out += ", "
		}
		out += e
	}
	return out
}

// CommandResult is the settlement of a Client command: the player's state/value
// on success, or an *Error on failure.
type CommandResult struct {
	Value any
	Err   error
}

// Client is a transport-agnostic port of the SDK's Player messaging core. It
// owns the handshake, the command queue that flushes once the embed answers
// hello, per-command timeouts and event dispatch. It performs no DOM work: the
// caller supplies a Transport that delivers bytes to the embed's frame.
type Client struct {
	transport      func(payload []byte) error
	commandTimeout time.Duration
	schedule       func(d time.Duration, f func()) (cancel func())
	sdkVersion     string

	mu         sync.Mutex
	connected  bool
	destroyed  bool
	queue      []*pendingCommand
	pending    map[int]*pendingCommand
	listeners  map[string][]listenerEntry
	nextID     int
	nextSub    uint64
	killKnock  func()
	knockTries int
	state      map[string]any

	settings any
	page     any
	ready    chan struct{}
	readySet sync.Once
}

type pendingCommand struct {
	id     int
	cmd    string
	args   any
	ch     chan CommandResult
	cancel func()
}

type listenerEntry struct {
	id uint64
	fn func(event string, data any)
}

// ClientOptions configures a Client.
type ClientOptions struct {
	// Transport delivers an encoded envelope to the embed. Required.
	Transport func(payload []byte) error
	// CommandTimeout defaults to CommandTimeout (15s).
	CommandTimeout time.Duration
	// SDKVersion is the sdk field of the hello message; defaults to Version.
	SDKVersion string
	// Settings, when non-nil, is sent in the hello message (the host page's
	// mirrored copy of the viewer's settings).
	Settings any
	// Page, when non-nil, is sent as the hello message's page field. The
	// original derives it from location.pathname unless reportPage is false;
	// here the caller decides.
	Page any
	// Schedule arranges a delayed call and returns a cancel function. It exists
	// so tests can drive the handshake and timeouts deterministically; the
	// default uses time.AfterFunc.
	Schedule func(d time.Duration, f func()) (cancel func())
}

// NewClient creates a Client. It does not start the handshake; call Handshake.
func NewClient(opts ClientOptions) *Client {
	sched := opts.Schedule
	if sched == nil {
		sched = func(d time.Duration, f func()) func() {
			t := time.AfterFunc(d, f)
			return func() { t.Stop() }
		}
	}
	timeout := opts.CommandTimeout
	if timeout <= 0 {
		timeout = CommandTimeout
	}
	version := opts.SDKVersion
	if version == "" {
		version = Version
	}
	return &Client{
		transport:      opts.Transport,
		commandTimeout: timeout,
		schedule:       sched,
		sdkVersion:     version,
		pending:        map[int]*pendingCommand{},
		listeners:      map[string][]listenerEntry{},
		state:          map[string]any{},
		nextID:         1,
		settings:       opts.Settings,
		page:           opts.Page,
		ready:          make(chan struct{}),
	}
}

// Ready returns a channel closed when the embed has answered the handshake.
func (c *Client) Ready() <-chan struct{} { return c.ready }

// Connected reports whether the embed has answered hello.
func (c *Client) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

// State returns a copy of the merged player state received so far.
func (c *Client) State() map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]any, len(c.state))
	for k, v := range c.state {
		out[k] = v
	}
	return out
}

func (c *Client) sendLocked(env Envelope) {
	if c.transport == nil {
		return
	}
	env.NS = Namespace
	env.V = APIProtocol
	payload, err := json.Marshal(env)
	if err != nil {
		return
	}
	_ = c.transport(payload)
}

// Handshake starts the knock loop: it sends hello every 300ms until the embed
// answers (up to 21 attempts, the SDK's tries++ > 20).
func (c *Client) Handshake() {
	c.mu.Lock()
	c.knockLocked()
	c.mu.Unlock()
}

func (c *Client) knockLocked() {
	if c.connected || c.destroyed || c.knockTries > 20 {
		return
	}
	c.knockTries++
	args := map[string]any{"sdk": c.sdkVersion}
	if c.settings != nil {
		args["settings"] = c.settings
	}
	if c.page != nil {
		args["page"] = c.page
	}
	c.sendLocked(Envelope{Cmd: "hello", ID: intPtr(0), Args: args})
	c.killKnock = c.schedule(300*time.Millisecond, func() {
		c.mu.Lock()
		c.knockLocked()
		c.mu.Unlock()
	})
}

// Command queues a command, mirroring the SDK's _command: it settles as soon as
// the embed answers, or times out after the command timeout, or is rejected
// once the client is destroyed. The returned channel receives exactly one
// value.
func (c *Client) Command(cmd string, args any) <-chan CommandResult {
	ch := make(chan CommandResult, 1)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.destroyed {
		ch <- CommandResult{Err: newError("destroyed", "This player was destroyed.")}
		return ch
	}
	id := c.nextID
	c.nextID++
	pc := &pendingCommand{id: id, cmd: cmd, args: args, ch: ch}
	pc.cancel = c.schedule(c.commandTimeout, func() { c.timeout(id) })
	c.pending[id] = pc
	if c.connected {
		c.sendLocked(Envelope{Cmd: cmd, ID: intPtr(id), Args: args})
	} else {
		c.queue = append(c.queue, pc)
	}
	return ch
}

func (c *Client) timeout(id int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pc, ok := c.pending[id]
	if !ok {
		return
	}
	delete(c.pending, id)
	pc.ch <- CommandResult{Err: newError("timeout", "The player did not answer "+pc.cmd+" in time.")}
}

// Receive processes one inbound postMessage payload from the embed. Unknown
// namespaces/versions and malformed JSON are ignored (nil); a JSON decode error
// is returned so callers can log it. The origin/source checks the browser SDK
// performs are the transport's responsibility.
func (c *Client) Receive(payload []byte) error {
	var env Envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return err
	}
	if env.NS != Namespace || env.V != APIProtocol {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.destroyed {
		return nil
	}
	if env.ID != nil && *env.ID == 0 && env.OK != nil && *env.OK {
		if c.connected {
			return nil
		}
		c.connected = true
		if st := asMap(env.Result); st != nil {
			if m, ok := st["state"]; ok {
				c.merge(asMap(m))
			}
		}
		for _, pc := range c.queue {
			c.sendLocked(Envelope{Cmd: pc.cmd, ID: intPtr(pc.id), Args: pc.args})
		}
		c.queue = nil
		c.readySet.Do(func() { close(c.ready) })
		return nil
	}
	if env.ID != nil {
		if pc, ok := c.pending[*env.ID]; ok {
			delete(c.pending, *env.ID)
			if pc.cancel != nil {
				pc.cancel()
			}
			if env.OK != nil && *env.OK {
				if st := asMap(env.Result); st != nil {
					c.merge(st)
				}
				pc.ch <- CommandResult{Value: env.Result}
			} else {
				code := "command-failed"
				var msg string
				if env.Error != nil {
					if env.Error.Code != "" {
						code = env.Error.Code
					}
					msg = env.Error.Message
				}
				pc.ch <- CommandResult{Err: newError(code, msg)}
			}
			return nil
		}
	}
	if env.Event != "" {
		if st := asMap(env.Data); st != nil {
			c.merge(st)
		}
		c.emitLocked(env.Event, env.Data)
	}
	return nil
}

// On subscribes to an event and returns an unsubscribe function (the SDK's
// on() return value). event may be "*" to receive every event; other names must
// be known. The listener receives (event, data); for a named subscription the
// event argument equals the subscription name.
func (c *Client) On(event string, listener func(event string, data any)) (func(), error) {
	if listener == nil {
		return nil, newError("invalid-listener", "Listener must be a function.")
	}
	if event != "*" && !KnownEvent(event) {
		return nil, newError("unknown-event", "Unknown event "+event+". Known: "+joinEvents())
	}
	c.mu.Lock()
	c.nextSub++
	id := c.nextSub
	c.listeners[event] = append(c.listeners[event], listenerEntry{id: id, fn: listener})
	c.mu.Unlock()
	return func() { c.removeListener(event, id) }, nil
}

// removeListener drops the listener with the given subscription id.
func (c *Client) removeListener(event string, id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.listeners[event]
	for i, l := range cur {
		if l.id == id {
			c.listeners[event] = append(cur[:i], cur[i+1:]...)
			return
		}
	}
}

// Once subscribes for a single delivery and returns an unsubscribe function.
func (c *Client) Once(event string, listener func(event string, data any)) (func(), error) {
	var off func()
	var err error
	off, err = c.On(event, func(name string, data any) {
		if off != nil {
			off()
		}
		listener(name, data)
	})
	return off, err
}

func (c *Client) emitLocked(event string, data any) {
	for _, l := range append(append([]listenerEntry{}, c.listeners[event]...), c.listeners["*"]...) {
		l.fn(event, data)
	}
}

func (c *Client) merge(m map[string]any) {
	if m == nil {
		return
	}
	for k, v := range m {
		c.state[k] = v
	}
}

// Destroy tears the client down, cancelling the knock loop and rejecting every
// outstanding command with a "destroyed" error.
func (c *Client) Destroy() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.destroyed {
		return
	}
	c.destroyed = true
	if c.killKnock != nil {
		c.killKnock()
	}
	for id, pc := range c.pending {
		if pc.cancel != nil {
			pc.cancel()
		}
		pc.ch <- CommandResult{Err: newError("destroyed", "This player was destroyed.")}
		delete(c.pending, id)
	}
	c.listeners = map[string][]listenerEntry{}
}

func asMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return nil
}
