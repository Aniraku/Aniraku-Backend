package anipm

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// manualSched is a deterministic scheduler for tests: delayed callbacks are
// recorded and fired explicitly by delay, so handshake and timeout behavior is
// observed without real timers.
type manualSched struct {
	mu    sync.Mutex
	items []*schedItem
}

type schedItem struct {
	d         time.Duration
	fn        func()
	cancelled bool
}

func (s *manualSched) schedule(d time.Duration, fn func()) func() {
	it := &schedItem{d: d, fn: fn}
	s.mu.Lock()
	s.items = append(s.items, it)
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		it.cancelled = true
		s.mu.Unlock()
	}
}

func (s *manualSched) fire(d time.Duration) {
	s.mu.Lock()
	var run []func()
	for _, it := range s.items {
		if !it.cancelled && it.d == d {
			run = append(run, it.fn)
		}
	}
	s.mu.Unlock()
	for _, fn := range run {
		fn()
	}
}

type recorder struct {
	mu   sync.Mutex
	msgs []Envelope
}

func (r *recorder) transport(payload []byte) error {
	var e Envelope
	if err := json.Unmarshal(payload, &e); err != nil {
		return err
	}
	r.mu.Lock()
	r.msgs = append(r.msgs, e)
	r.mu.Unlock()
	return nil
}

func (r *recorder) all() []Envelope {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Envelope(nil), r.msgs...)
}

func (r *recorder) find(cmd string) (Envelope, bool) {
	for _, e := range r.all() {
		if e.Cmd == cmd {
			return e, true
		}
	}
	return Envelope{}, false
}

func newTestClient(t *testing.T) (*Client, *recorder, *manualSched) {
	t.Helper()
	rec := &recorder{}
	sched := &manualSched{}
	c := NewClient(ClientOptions{Transport: rec.transport, Schedule: sched.schedule})
	return c, rec, sched
}

func feedHelloAck(t *testing.T, c *Client) {
	t.Helper()
	ack := Envelope{NS: Namespace, V: APIProtocol, ID: intPtr(0), OK: boolPtr(true),
		Result: map[string]any{"state": map[string]any{"status": "ready", "episode": float64(1)}}}
	b, _ := json.Marshal(ack)
	if err := c.Receive(b); err != nil {
		t.Fatalf("Receive(hello ack): %v", err)
	}
}

func TestClientHandshakeQueueAndReady(t *testing.T) {
	c, rec, _ := newTestClient(t)
	_ = c.Command("play", nil)
	if got := rec.all(); len(got) != 0 {
		t.Fatalf("command sent before handshake: %v", got)
	}
	c.Handshake()
	if hello, ok := rec.find("hello"); !ok || hello.ID == nil || *hello.ID != 0 {
		t.Fatalf("hello not sent correctly: %+v", rec.all())
	}
	if _, ok := rec.find("play"); ok {
		t.Fatalf("queued command sent before the embed answered")
	}
	feedHelloAck(t, c)
	select {
	case <-c.Ready():
	default:
		t.Fatalf("Ready not closed after hello ack")
	}
	if play, ok := rec.find("play"); !ok {
		t.Fatalf("queued command was not flushed after connect")
	} else if play.ID == nil || *play.ID == 0 {
		t.Fatalf("flushed command lost its id: %+v", play)
	}
	if c.State()["status"] != "ready" {
		t.Fatalf("state not merged: %v", c.State())
	}
}

func TestClientCommandResolveAndError(t *testing.T) {
	c, _, _ := newTestClient(t)
	c.Handshake()
	feedHelloAck(t, c)

	ok := c.Command("seek", map[string]any{"time": 5})
	answer := Envelope{NS: Namespace, V: APIProtocol, ID: intPtr(1), OK: boolPtr(true),
		Result: map[string]any{"status": "ready", "episode": float64(1), "currentTime": float64(5)}}
	b, _ := json.Marshal(answer)
	if err := c.Receive(b); err != nil {
		t.Fatal(err)
	}
	res := <-ok
	if res.Err != nil {
		t.Fatalf("seek failed: %v", res.Err)
	}

	bad := c.Command("next", nil)
	// id 2 is the next command
	answer = Envelope{NS: Namespace, V: APIProtocol, ID: intPtr(2), OK: boolPtr(false),
		Error: &WireError{Code: "no-next-episode", Message: "at the end"}}
	b, _ = json.Marshal(answer)
	c.Receive(b)
	res = <-bad
	e, okc := res.Err.(*Error)
	if !okc || e.Code != "no-next-episode" {
		t.Fatalf("next error = %v, want no-next-episode", res.Err)
	}
}

func TestClientCommandTimeout(t *testing.T) {
	c, _, sched := newTestClient(t)
	c.Handshake()
	feedHelloAck(t, c)
	ch := c.Command("loadEpisode", map[string]any{"episode": 2})
	sched.fire(CommandTimeout)
	select {
	case res := <-ch:
		e, ok := res.Err.(*Error)
		if !ok || e.Code != "timeout" {
			t.Fatalf("timeout result = %v", res.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("command did not time out")
	}
}

func TestClientDestroy(t *testing.T) {
	c, _, _ := newTestClient(t)
	ch := c.Command("play", nil) // queued
	c.Destroy()
	res := <-ch
	if e, ok := res.Err.(*Error); !ok || e.Code != "destroyed" {
		t.Fatalf("pending command after destroy = %v", res.Err)
	}
	ch2 := c.Command("pause", nil)
	res2 := <-ch2
	if e, ok := res2.Err.(*Error); !ok || e.Code != "destroyed" {
		t.Fatalf("command after destroy = %v", res2.Err)
	}
}

func TestClientEvents(t *testing.T) {
	c, _, _ := newTestClient(t)
	var named, wild int
	off, err := c.On("play", func(name string, data any) { named++ })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.On("*", func(name string, data any) { wild++ }); err != nil {
		t.Fatal(err)
	}
	send := func(event string) {
		b, _ := json.Marshal(Envelope{NS: Namespace, V: APIProtocol, Event: event, Data: map[string]any{"episode": float64(1)}})
		c.Receive(b)
	}
	send("play")
	send("pause")
	if named != 1 {
		t.Errorf("named listener calls = %d, want 1", named)
	}
	if wild != 2 {
		t.Errorf("wildcard listener calls = %d, want 2", wild)
	}
	off()
	send("play")
	if named != 1 {
		t.Errorf("named listener fired after unsubscribe: %d", named)
	}

	if _, err := c.On("bogus", func(string, any) {}); err == nil {
		t.Errorf("On(bogus) did not error")
	} else if e, ok := err.(*Error); !ok || e.Code != "unknown-event" {
		t.Errorf("On(bogus) error = %v, want unknown-event", err)
	}
	if _, err := c.On("play", nil); err == nil {
		t.Errorf("On(play, nil) did not error")
	}
}

func TestClientReceiveGuards(t *testing.T) {
	c, _, _ := newTestClient(t)
	if err := c.Receive([]byte(`{"ns":"other","v":1,"event":"play"}`)); err != nil {
		t.Errorf("foreign envelope returned error: %v", err)
	}
	if err := c.Receive([]byte(`{"ns":"anipm.player","v":2,"event":"play"}`)); err != nil {
		t.Errorf("wrong version returned error: %v", err)
	}
	if err := c.Receive([]byte(`not json`)); err == nil {
		t.Errorf("malformed payload did not return an error")
	}
}

func TestKnownEventAndJoinEvents(t *testing.T) {
	if !KnownEvent("timeupdate") || KnownEvent("nope") {
		t.Fatalf("KnownEvent misclassified")
	}
	if len(Events) != 26 {
		t.Fatalf("Events len = %d, want 26", len(Events))
	}
}
