package streaming

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// dubSubFakeProvider scripts FindEpisodeSource per lang and counts calls.
type dubSubFakeProvider struct {
	name     string
	dub      *SourceResult
	sub      *SourceResult
	subErr   error
	subCalls *int
}

func (f *dubSubFakeProvider) Name() string { return f.name }
func (f *dubSubFakeProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, nil
}
func (f *dubSubFakeProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, nil
}
func (f *dubSubFakeProvider) FindEpisodeSource(ctx context.Context, providerID string, episode int, lang string) (*SourceResult, error) {
	if lang == "sub" {
		*f.subCalls++
		return f.sub, f.subErr
	}
	return f.dub, nil
}

func dubSubTestManager(fake *dubSubFakeProvider) *Manager {
	return &Manager{log: zerolog.Nop(), providers: []Provider{fake}}
}

func dubSubSrc(url string, subs ...core.Subtitle) core.Source {
	return core.Source{URL: url, Type: "hls", Quality: "auto", Subtitles: subs}
}

// Non-dub requests never trigger a sub fetch.
func TestDubSubtitlesSubPassthrough(t *testing.T) {
	calls := 0
	fake := &dubSubFakeProvider{name: "flixcloud", subCalls: &calls}
	m := dubSubTestManager(fake)
	in := &SourceResult{Sources: []core.Source{dubSubSrc("u1")}}
	if got := m.withDubSubtitles(context.Background(), "flixcloud", "sub", "1", 1, in); got != in {
		t.Fatal("sub request must return the input untouched")
	}
	if calls != 0 {
		t.Fatalf("sub fetch called %d times, want 0", calls)
	}
}

// Dub sources that already have subtitles cost zero extra fetches.
func TestDubSubtitlesNoFetchWhenPresent(t *testing.T) {
	calls := 0
	fake := &dubSubFakeProvider{name: "zoko", subCalls: &calls}
	m := dubSubTestManager(fake)
	in := &SourceResult{Sources: []core.Source{
		dubSubSrc("u1", core.Subtitle{URL: "s1", Lang: "dub", Label: "dub"}),
	}}
	if got := m.withDubSubtitles(context.Background(), "zoko", "dub", "1", 1, in); got != in {
		t.Fatal("complete dub result must return the input untouched")
	}
	if calls != 0 {
		t.Fatalf("sub fetch called %d times, want 0", calls)
	}
}

// Dub sources missing subtitles are filled from the sub resolve (deduped),
// and the input is never mutated (provider caches may share it).
func TestDubSubtitlesFilledFromSub(t *testing.T) {
	calls := 0
	fake := &dubSubFakeProvider{
		name:     "anikoto",
		subCalls: &calls,
		dub: &SourceResult{Sources: []core.Source{
			dubSubSrc("u1"),
			dubSubSrc("u2", core.Subtitle{URL: "keep", Lang: "dub", Label: "dub"}),
		}},
		sub: &SourceResult{Sources: []core.Source{
			dubSubSrc("s1",
				core.Subtitle{URL: "a", Lang: "sub", Label: "sub"},
				core.Subtitle{URL: "b", Lang: "sub", Label: "sub"},
				core.Subtitle{URL: "a", Lang: "sub", Label: "dup"}),
		}},
	}
	m := dubSubTestManager(fake)
	in := fake.dub
	got := m.withDubSubtitles(context.Background(), "anikoto", "dub", "1", 1, in)
	if calls != 1 {
		t.Fatalf("sub fetch called %d times, want 1", calls)
	}
	if len(got.Sources) != 2 {
		t.Fatalf("sources = %d, want 2", len(got.Sources))
	}
	if len(got.Sources[0].Subtitles) != 2 || got.Sources[0].Subtitles[0].URL != "a" || got.Sources[0].Subtitles[1].URL != "b" {
		t.Fatalf("filled subs = %+v, want [a b]", got.Sources[0].Subtitles)
	}
	if len(got.Sources[1].Subtitles) != 1 || got.Sources[1].Subtitles[0].URL != "keep" {
		t.Fatalf("existing subs touched: %+v", got.Sources[1].Subtitles)
	}
	if len(in.Sources[0].Subtitles) != 0 {
		t.Fatal("input mutated: provider-shared results must never be modified")
	}
}

// Failed sub resolve keeps the dub result as-is.
func TestDubSubtitlesSubFailureKeepsDub(t *testing.T) {
	calls := 0
	fake := &dubSubFakeProvider{
		name:     "nin",
		subCalls: &calls,
		dub:      &SourceResult{Sources: []core.Source{dubSubSrc("u1")}},
		subErr:   errors.New("boom"),
	}
	m := dubSubTestManager(fake)
	in := fake.dub
	if got := m.withDubSubtitles(context.Background(), "nin", "dub", "1", 1, in); got != in {
		t.Fatal("failed sub fetch must return the input untouched")
	}
	if calls != 1 {
		t.Fatalf("sub fetch called %d times, want 1", calls)
	}
}

// Nil input stays nil without touching providers.
func TestDubSubtitlesNil(t *testing.T) {
	calls := 0
	fake := &dubSubFakeProvider{name: "kaa", subCalls: &calls}
	m := dubSubTestManager(fake)
	if got := m.withDubSubtitles(context.Background(), "kaa", "dub", "1", 1, nil); got != nil {
		t.Fatal("nil input must stay nil")
	}
	if calls != 0 {
		t.Fatalf("sub fetch called %d times, want 0", calls)
	}
}
