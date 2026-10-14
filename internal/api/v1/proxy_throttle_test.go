package v1

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func TestShapeWriterDisabledByDefault(t *testing.T) {
	t.Setenv(shapeEnv, "")
	var buf bytes.Buffer
	w := shapeWriter(&buf)
	if _, ok := w.(*paceWriter); ok {
		t.Fatal("shaping must be off without the env var")
	}
	if _, err := io.WriteString(w, "hello"); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "hello" {
		t.Fatalf("passthrough corrupted: %q", buf.String())
	}
}

func TestShapeWriterRejectsBadEnv(t *testing.T) {
	for _, v := range []string{"abc", "-5", "0"} {
		t.Setenv(shapeEnv, v)
		var buf bytes.Buffer
		if _, ok := shapeWriter(&buf).(*paceWriter); ok {
			t.Fatalf("env %q must disable shaping", v)
		}
	}
}

func TestShapeWriterBurstIsFree(t *testing.T) {
	t.Setenv(shapeEnv, "100000") // 100KB/s; burst (256KB) covers this write
	var buf bytes.Buffer
	w := shapeWriter(&buf)
	body := bytes.Repeat([]byte("a"), 200<<10)
	start := time.Now()
	if _, err := w.Write(body); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("200KB inside the burst took %v, must pass at full speed", elapsed)
	}
	if buf.Len() != len(body) || !bytes.Equal(buf.Bytes(), body) {
		t.Fatal("burst bytes corrupted")
	}
}

func TestShapeWriterPacesBeyondBurst(t *testing.T) {
	t.Setenv(shapeEnv, "100000") // 100KB/s
	var buf bytes.Buffer
	w := shapeWriter(&buf)
	body := bytes.Repeat([]byte("b"), 300<<10)
	start := time.Now()
	n, err := w.Write(body)
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	// 44KB past the 256KB burst at 100KB/s sleeps ~440ms; floor assertion
	// only (scheduling can only add time, never remove the sleep).
	if elapsed < 300*time.Millisecond {
		t.Fatalf("300KB at 100KB/s with 256KB burst took %v, pacing did not engage", elapsed)
	}
	if n != len(body) || buf.Len() != len(body) {
		t.Fatalf("paced write corrupted: n=%d buf=%d want %d", n, buf.Len(), len(body))
	}
}
