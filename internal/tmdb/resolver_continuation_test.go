package tmdb

import "testing"

// identityAlignedContinuation gates the 1:1 extension of verified mappings
// past AniBridge's verified frontier (airing shows). Only the plain
// fresh-show shape may ever qualify; everything exotic must be rejected so
// verified sequel/mid-season semantics stay untouched.
func TestIdentityAlignedContinuation(t *testing.T) {
	cases := []struct {
		name    string
		ranges  map[string]string
		wantOff int
		wantEnd int
		wantOK  bool
	}{
		{"identity short", map[string]string{"1-2": "1-2"}, 0, 2, true},
		{"identity cour", map[string]string{"1-12": "1-12"}, 0, 12, true},
		{"singleton", map[string]string{"1": "1"}, 0, 1, true},
		{"second range not starting at 1 rejected", map[string]string{"1-2": "5-6", "3-4": "7-8"}, 0, 0, false},
		{"mid-season start rejected", map[string]string{"10-22": "1-12"}, 0, 0, false},
		{"injected singleton rejected", map[string]string{"1-2": "1-2", "3": "3"}, 0, 0, false},
		{"open ended rejected", map[string]string{"1-": "1-"}, 0, 0, false},
		{"non constant offset rejected", map[string]string{"1-2": "1-3"}, 0, 0, false},
		{"empty rejected", map[string]string{}, 0, 0, false},
		{"garbage rejected", map[string]string{"abc": "def"}, 0, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			off, end, ok := identityAlignedContinuation(tc.ranges)
			if ok != tc.wantOK || (ok && (off != tc.wantOff || end != tc.wantEnd)) {
				t.Fatalf("identityAlignedContinuation(%v) = (%d, %d, %v), want (%d, %d, %v)",
					tc.ranges, off, end, ok, tc.wantOff, tc.wantEnd, tc.wantOK)
			}
		})
	}
}
