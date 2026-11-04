package streaming

import (
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// outageBreaker is the shared network-outage circuit: while a provider's
// upstream hangs, every /servers call used to wait the full collector
// budget for its timeouts (novuscloud.buzz hung 1251 GETs in 2h; NiN and
// AniWaves show the same shape on their edges). After a hang strike the
// provider pauses fail-fast (zero results, zero wait - exactly what a
// failing run delivered); the pause doubles per strike up to the cap, and
// one healthy resolve lifts it instantly.
//
// Health is classified by RESOLVE DURATION at the collector call site:
// a resolve that ran past probeMin burned its own budget on a hung
// connection; fast failures (404s, no-match) are the server answering and
// count as healthy.
type outageBreaker struct {
	mu       sync.Mutex
	until    time.Time
	strikes  int
	name     string
	log      zerolog.Logger
	probeMin time.Duration
	base     time.Duration
	max      time.Duration
}

func newOutageBreaker(name string, log zerolog.Logger, probeMin time.Duration) outageBreaker {
	return outageBreaker{name: name, log: log, probeMin: probeMin, base: 10 * time.Minute, max: time.Hour}
}

func (b *outageBreaker) paused() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return time.Now().Before(b.until)
}

// noteResolve records one resolve outcome by duration.
func (b *outageBreaker) noteResolve(d time.Duration) {
	b.note(d >= b.probeMin)
}

func (b *outageBreaker) note(healthy bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if healthy {
		if b.strikes > 0 {
			b.log.Info().Str("provider", b.name).Msg("outage pause lifted (upstream healthy again)")
		}
		b.strikes = 0
		b.until = time.Time{}
		return
	}
	b.strikes++
	d := b.base << (b.strikes - 1)
	if d > b.max || d <= 0 {
		d = b.max
	}
	b.until = time.Now().Add(d)
	b.log.Warn().Str("provider", b.name).Int("strike", b.strikes).
		Dur("pause", d).Msg("upstream hung, outage pause opened")
}
