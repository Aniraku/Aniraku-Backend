package v1

import (
	"os"
	"path/filepath"
	"testing"
)

// TestMain routes the dynamic-CDN learner's persistence to a temp dir for
// the whole test run: the production default (data/dynamic_cdn.json) is
// relative to the service working directory, and without this redirect any
// test that vouches a host writes its snapshot into the package directory.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "aniraku-allowlist-test")
	if err == nil {
		os.Setenv("ANIRAKU_DYNAMIC_CDN_FILE", filepath.Join(dir, "dynamic_cdn.json"))
		defer os.RemoveAll(dir)
	}
	os.Exit(m.Run())
}
