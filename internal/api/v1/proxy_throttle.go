package v1

import (
	"io"
	"os"
	"strconv"
	"time"
)

// Emergency origin-egress shaping (temporary; review schedule in docs/ops-notes.md).
//
// Azure bills every byte the origin serves, and Cloudflare only absorbs
// repeat pulls, first views always come from this box. While the throttle
// env is set, large proxy bodies pace to that rate after a small burst:
// players' ABR measures the slower fill and settles ~one rung lower on its
// own. No URL, header, or status semantics change; small bodies (errors,
// playlists, subtitles, keys) fit inside the burst and pass at full speed,
// so provider-fallback chains and channel switching stay fast. Edge HITs
// are unaffected (full Cloudflare speed), only origin fills are slowed.
//
// Revert: unset ANIRAKU_PROXY_SHAPE_BPS and restart (or delete this file
// and the two shaping call sites).
const (
	shapeEnv       = "ANIRAKU_PROXY_SHAPE_BPS"
	shapeBurstByte = 256 << 10
)

// shapeRateBps reads the throttle per response (cheap enough to allow live
// tuning via env + restart). <=0 disables shaping entirely.
func shapeRateBps() int64 {
	n, err := strconv.ParseInt(os.Getenv(shapeEnv), 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

// paceWriter passes the first burst bytes at full speed, then sleeps to hold
// the average at bytesPerSec. Sleeps are per-Write (io.Copy chunks ~32KB,
// so each sleep is milliseconds); a client disconnect surfaces as a failed
// Write right after, ending the copy.
type paceWriter struct {
	w           io.Writer
	bytesPerSec int64
	burstLeft   int64
}

func shapeWriter(w io.Writer) io.Writer {
	bps := shapeRateBps()
	if bps <= 0 {
		return w
	}
	return &paceWriter{w: w, bytesPerSec: bps, burstLeft: shapeBurstByte}
}

func (p *paceWriter) Write(b []byte) (int, error) {
	if p.bytesPerSec <= 0 || len(b) == 0 {
		return p.w.Write(b)
	}
	excess := int64(len(b))
	if p.burstLeft > 0 {
		if excess <= p.burstLeft {
			p.burstLeft -= excess
			return p.w.Write(b)
		}
		excess -= p.burstLeft
		p.burstLeft = 0
	}
	if excess > 0 {
		time.Sleep(time.Duration(excess) * time.Second / time.Duration(p.bytesPerSec))
	}
	return p.w.Write(b)
}
