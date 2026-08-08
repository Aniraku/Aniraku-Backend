package streaming

import (
	"testing"
)

func TestSkipFromAniskip(t *testing.T) {
	entries := []miruroAniskip{
		{Episode: 1, Type: "op", Start: 90, End: 150, Votes: 2},
		{Episode: 1, Type: "op", Start: 92, End: 148, Votes: 5},
		{Episode: 1, Type: "op", Start: 90, End: 150, Votes: -1},
		{Episode: 1, Type: "ed", Start: 1380, End: 1435, Votes: 1},
		{Episode: 2, Type: "op", Start: 85, End: 140, Votes: 3},
		{Episode: 1, Type: "op", Start: 0, End: 90, Votes: 9},
		{Episode: 1, Type: "op", Start: 10, End: 5, Votes: 9},
	}

	intro, outro := skipFromAniskip(entries, 1)
	if intro == nil || intro.Start != 92 || intro.End != 148 {
		t.Fatalf("expected highest-voted intro, got %+v", intro)
	}
	if outro == nil || outro.Start != 1380 || outro.End != 1435 {
		t.Fatalf("expected outro, got %+v", outro)
	}

	intro2, outro2 := skipFromAniskip(entries, 3)
	if intro2 != nil || outro2 != nil {
		t.Fatalf("expected nil segments for unknown episode, got %+v / %+v", intro2, outro2)
	}
}

func TestSkipFromAniskipPrefersVotedOverMixed(t *testing.T) {
	entries := []miruroAniskip{
		{Episode: 1, Type: "op", Start: 90, End: 150, Votes: -1},
		{Episode: 1, Type: "op", Start: 100, End: 155, Votes: -2},
	}
	intro, _ := skipFromAniskip(entries, 1)
	if intro == nil {
		t.Fatal("expected a fallback intro when only negative-voted entries exist")
	}
	if intro.Start != 90 {
		t.Fatalf("expected least-negative fallback, got %+v", intro)
	}
}
