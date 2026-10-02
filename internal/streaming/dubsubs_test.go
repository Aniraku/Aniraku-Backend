package streaming

import (
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// Operator rule: ONLY Sora (animex) dub sources carry the Nico (kaa)
// subtitle files — Sora serves krussdomi streams, so Nico subs are
// timing-compatible. Every other provider's dub keeps its defaults.
// These tests use the real kaa fixture provider as the subtitle origin
// (anilist "20" ep1 sub resolves one Nico source with vtt-sub.vtt).
func dubNicoTestManager(t *testing.T) *Manager {
	t.Helper()
	f := newKaaFixture(t)
	return &Manager{log: zerolog.Nop(), providers: []Provider{newKaaTestProvider(f)}}
}

func dubSrc(url string, subs ...core.Subtitle) core.Source {
	return core.Source{URL: url, Type: "hls", Quality: "auto", Subtitles: subs}
}

func soraDubResult() *SourceResult {
	return &SourceResult{
		ServerName: "Sora",
		Sources: []core.Source{
			dubSrc("https://sora.example/a.m3u8",
				core.Subtitle{URL: "https://sora.example/a-eng.vtt", Lang: "en", Label: "English"}),
			dubSrc("https://sora.example/b.m3u8"),
		},
	}
}

// Sora dub sources get the Nico files; input untouched.
func TestDubSubtitlesNicoAppliedToSora(t *testing.T) {
	m := dubNicoTestManager(t)
	in := soraDubResult()
	got := m.withDubSubtitles(kaaTestCtx(t), "animex", "dub", "20", 1, in)
	if got == in {
		t.Fatal("expected a rewritten copy, got the input pointer")
	}
	for i, src := range got.Sources {
		if len(src.Subtitles) != 1 || !strings.HasSuffix(src.Subtitles[0].URL, "/vtt-sub.vtt") {
			t.Fatalf("sources[%d] subs = %+v, want the single Nico file vtt-sub.vtt", i, src.Subtitles)
		}
	}
	if len(in.Sources[0].Subtitles) != 1 || len(in.Sources[1].Subtitles) != 0 {
		t.Fatal("input mutated: provider-shared results must never be modified")
	}
}

// Non-Sora animex servers (Mochi, ...) keep their defaults.
func TestDubSubtitlesNonSoraUntouched(t *testing.T) {
	m := dubNicoTestManager(t)
	in := &SourceResult{
		ServerName: "Mochi",
		Sources: []core.Source{dubSrc("https://mochi.example/a.m3u8",
			core.Subtitle{URL: "https://mochi.example/a-eng.vtt", Lang: "en", Label: "English"})},
	}
	if got := m.withDubSubtitles(kaaTestCtx(t), "animex", "dub", "20", 1, in); got != in {
		t.Fatal("non-Sora server must return the input untouched")
	}
}

// Non-animex providers keep their defaults.
func TestDubSubtitlesNonAnimexUntouched(t *testing.T) {
	m := dubNicoTestManager(t)
	for _, tc := range []struct{ provider, server string }{
		{"anikoto", "Niko"},
		{"zoko", "Zoko"},
		{"nin", "NiN"},
		{"flixcloud", "Yuta"},
		{"kaa", "Nico"},
	} {
		in := &SourceResult{
			ServerName: tc.server,
			Sources: []core.Source{dubSrc("https://x.example/a.m3u8",
				core.Subtitle{URL: "https://x.example/a-eng.vtt", Lang: "en", Label: "English"})},
		}
		if got := m.withDubSubtitles(kaaTestCtx(t), tc.provider, "dub", "20", 1, in); got != in {
			t.Fatalf("%s dub must return the input untouched", tc.provider)
		}
	}
}

// Sora dub already carrying the Nico files comes back untouched.
func TestDubSubtitlesNicoIdenticalNoCopy(t *testing.T) {
	m := dubNicoTestManager(t)
	ksub, err := m.providers[0].FindEpisodeSource(kaaTestCtx(t), "20", 1, "sub")
	if err != nil || len(ksub.Sources) == 0 || len(ksub.Sources[0].Subtitles) == 0 {
		t.Fatalf("Nico resolve: %v %+v", err, ksub)
	}
	in := &SourceResult{
		ServerName: "Sora",
		Sources:    []core.Source{dubSrc("https://sora.example/a.m3u8", ksub.Sources[0].Subtitles...)},
	}
	if got := m.withDubSubtitles(kaaTestCtx(t), "animex", "dub", "20", 1, in); got != in {
		t.Fatal("identical files must return the input untouched")
	}
}

// No kaa provider configured: Sora dub untouched, no error.
func TestDubSubtitlesNoKaaKeepsDub(t *testing.T) {
	m := &Manager{log: zerolog.Nop(), providers: nil}
	in := soraDubResult()
	if got := m.withDubSubtitles(kaaTestCtx(t), "animex", "dub", "20", 1, in); got != in {
		t.Fatal("missing kaa must return the input untouched")
	}
}

// kaa without a match (empty listing): Sora dub untouched, no error.
func TestDubSubtitlesNicoMissingKeepsDub(t *testing.T) {
	f := newKaaFixture(t)
	f.emptyEpisodes = true
	m := &Manager{log: zerolog.Nop(), providers: []Provider{newKaaTestProvider(f)}}
	in := soraDubResult()
	if got := m.withDubSubtitles(kaaTestCtx(t), "animex", "dub", "20", 1, in); got != in {
		t.Fatal("unavailable Nico must return the input untouched")
	}
}

// Non-dub requests never touch kaa.
func TestDubSubtitlesSubPassthrough(t *testing.T) {
	m := dubNicoTestManager(t)
	in := soraDubResult()
	if got := m.withDubSubtitles(kaaTestCtx(t), "animex", "sub", "20", 1, in); got != in {
		t.Fatal("sub request must return the input untouched")
	}
}

// Nil input stays nil without touching providers.
func TestDubSubtitlesNil(t *testing.T) {
	m := dubNicoTestManager(t)
	if got := m.withDubSubtitles(kaaTestCtx(t), "animex", "dub", "20", 1, nil); got != nil {
		t.Fatal("nil input must stay nil")
	}
}

// mergeNiNSubtitles on dub must preserve existing subtitles and only fill
// sources that have none; sub keeps the legacy overwrite-from-donor
// behavior.
func TestMergeNiNSubtitlesDubPreservesExisting(t *testing.T) {
	own := []core.Subtitle{{URL: "https://nin.example/d.vtt", Lang: "en", Label: "English"}}
	donor := []core.Subtitle{{URL: "https://ak.example/d.vtt", Lang: "en", Label: "English"}}
	nn := []core.Server{{
		Name: "NiN", Provider: "nin", Lang: "dub",
		Sources: []core.Source{dubSrc("https://nin.example/d.m3u8", own...), dubSrc("https://nin.example/e.m3u8")},
	}}
	ak := []core.Server{{
		Name: "Niko", Provider: "anikoto", Lang: "dub",
		Sources: []core.Source{dubSrc("https://ak.example/d.m3u8", donor...)},
	}}
	got := mergeNiNSubtitles(nn, ak, nil, nil, "dub")
	if len(got[0].Sources[0].Subtitles) != 1 || got[0].Sources[0].Subtitles[0].URL != "https://nin.example/d.vtt" {
		t.Fatalf("existing dub subs overwritten: %+v", got[0].Sources[0].Subtitles)
	}
	if len(got[0].Sources[1].Subtitles) != 1 || got[0].Sources[1].Subtitles[0].URL != "https://ak.example/d.vtt" {
		t.Fatalf("empty dub source not filled from donor: %+v", got[0].Sources[1].Subtitles)
	}
	if &got[0].Sources[1].Subtitles[0] == &donor[0] {
		t.Fatal("donor slice aliased into the server instead of copied")
	}
}

func TestMergeNiNSubtitlesSubOverwrites(t *testing.T) {
	own := []core.Subtitle{{URL: "https://nin.example/s.vtt", Lang: "en", Label: "English"}}
	donor := []core.Subtitle{{URL: "https://ak.example/s.vtt", Lang: "en", Label: "English"}}
	nn := []core.Server{{
		Name: "NiN", Provider: "nin", Lang: "sub",
		Sources: []core.Source{dubSrc("https://nin.example/s.m3u8", own...)},
	}}
	ak := []core.Server{{
		Name: "Niko", Provider: "anikoto", Lang: "sub",
		Sources: []core.Source{dubSrc("https://ak.example/s.m3u8", donor...)},
	}}
	got := mergeNiNSubtitles(nn, ak, nil, nil, "sub")
	if len(got[0].Sources[0].Subtitles) != 1 || got[0].Sources[0].Subtitles[0].URL != "https://ak.example/s.vtt" {
		t.Fatalf("sub donor overwrite changed: %+v", got[0].Sources[0].Subtitles)
	}
}
