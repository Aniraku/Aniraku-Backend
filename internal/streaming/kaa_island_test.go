package streaming

import (
	"strings"
	"testing"
)

// Real props attribute captured from a krussdomi cat-player page
// (one-piece ep1): manifest, thumbnails, and four subtitle tracks, each
// tagged as [0, value] with arrays as [1, elements].
const kaaIslandProps = "{&quot;manifest&quot;:[0,&quot;//bl.krussdomi.com/playlist/679705bd169c31976bd92812/master.m3u8&quot;],&quot;thumbnails&quot;:[0,&quot;https:///subbl.krussdomi.com/679705bd169c31976bd92812/preview-4koE-.vtt&quot;],&quot;subtitles&quot;:[1,[[0,{&quot;format&quot;:[0,&quot;srt&quot;],&quot;language&quot;:[0,&quot;en&quot;],&quot;filename&quot;:[0,&quot;1617629131445_en.srt&quot;],&quot;name&quot;:[0,&quot;English&quot;],&quot;source&quot;:[0,&quot;bilibili&quot;],&quot;src&quot;:[0,&quot;https:///subbl.krussdomi.com/679705bd169c31976bd92812/1617629131445_en.srt&quot;]}],[0,{&quot;format&quot;:[0,&quot;srt&quot;],&quot;language&quot;:[0,&quot;th&quot;],&quot;filename&quot;:[0,&quot;1617629103629_th.srt&quot;],&quot;name&quot;:[0,&quot;ภาษาไทย&quot;],&quot;source&quot;:[0,&quot;bilibili&quot;],&quot;src&quot;:[0,&quot;https:///subbl.krussdomi.com/679705bd169c31976bd92812/1617629103629_th.srt&quot;]}],[0,{&quot;format&quot;:[0,&quot;srt&quot;],&quot;language&quot;:[0,&quot;vi&quot;],&quot;filename&quot;:[0,&quot;1617629161843_vi.srt&quot;],&quot;name&quot;:[0,&quot;Tiếng Việt&quot;],&quot;source&quot;:[0,&quot;bilibili&quot;],&quot;src&quot;:[0,&quot;https:///subbl.krussdomi.com/679705bd169c31976bd92812/1617629161843_vi.srt&quot;]}],[0,{&quot;format&quot;:[0,&quot;srt&quot;],&quot;language&quot;:[0,&quot;id&quot;],&quot;filename&quot;:[0,&quot;1617351264757_id.srt&quot;],&quot;name&quot;:[0,&quot;Bahasa Indonesia&quot;],&quot;source&quot;:[0,&quot;bilibili&quot;],&quot;src&quot;:[0,&quot;https:///subbl.krussdomi.com/679705bd169c31976bd92812/1617351264757_id.srt&quot;]}]]],&quot;sourceId&quot;:[0,&quot;ZWJlNzE1NDNhMWE3NmQ5OWM3NTlhNzNlNjU4M2Y4ZTc6ZGMyOWUyMjg2NWI3MGUzY2ZkNGE3MThlYWEwNjQ0OGE&quot;],&quot;sourceType&quot;:[0,&quot;catstream&quot;],&quot;debug&quot;:[0,false]}"

// Second real shape (BLEACH ep9, VidStreaming player): tracks without a
// format field and with three-letter ISO 639-2 language codes.
const kaaIslandPropsNoFormat = "{&quot;manifest&quot;:[0,&quot;https://hls.krussdomi.com/manifest/6ad63ab2ebaa068fefb7bd87/master.m3u8&quot;],&quot;subtitles&quot;:[1,[[0,{&quot;language&quot;:[0,&quot;eng&quot;],&quot;name&quot;:[0,&quot;English&quot;],&quot;src&quot;:[0,&quot;https://subst.krussdomi.com/6ad63ab2ebaa068fefb7bd87/6ad63ab7cf11dabbe3742bda.vtt&quot;]}],[0,{&quot;language&quot;:[0,&quot;ger&quot;],&quot;name&quot;:[0,&quot;German&quot;],&quot;src&quot;:[0,&quot;https://subst.krussdomi.com/6ad63ab2ebaa068fefb7bd87/6ad63ab8cf11dabbe3742be4.vtt&quot;]}],[0,{&quot;language&quot;:[0,&quot;spa&quot;],&quot;name&quot;:[0,&quot;Spanish&quot;],&quot;src&quot;:[0,&quot;https://subst.krussdomi.com/6ad63ab2ebaa068fefb7bd87/6ad63ab8cf11dabbe3742bee.vtt&quot;]}],[0,{&quot;language&quot;:[0,&quot;fre&quot;],&quot;name&quot;:[0,&quot;French&quot;],&quot;src&quot;:[0,&quot;https://subst.krussdomi.com/6ad63ab2ebaa068fefb7bd87/6ad63ab9cf11dabbe3742bf8.vtt&quot;]}],[0,{&quot;language&quot;:[0,&quot;ita&quot;],&quot;name&quot;:[0,&quot;Italian&quot;],&quot;src&quot;:[0,&quot;https://subst.krussdomi.com/6ad63ab2ebaa068fefb7bd87/6ad63ab9cf11dabbe3742c02.vtt&quot;]}],[0,{&quot;language&quot;:[0,&quot;por&quot;],&quot;name&quot;:[0,&quot;Portuguese&quot;],&quot;src&quot;:[0,&quot;https://subst.krussdomi.com/6ad63ab2ebaa068fefb7bd87/6ad63abacf11dabbe3742c0c.vtt&quot;]}]]]}"

func TestKaaIslandSubtitleMetadataNoFormat(t *testing.T) {
	page := `<astro-island props="` + kaaIslandPropsNoFormat + `" ssr client="only">`
	metas := kaaParseSubtitleMeta(page)
	if len(metas) != 6 {
		t.Fatalf("got %d subtitle metas, want 6: %+v", len(metas), metas)
	}
	wantLang := []string{"en", "de", "es", "fr", "it", "pt"}
	for i, m := range metas {
		sub := buildSubtitle(m.src, m.lang, m.name, "")
		if sub.Lang != wantLang[i] {
			t.Errorf("track %d lang = %q, want %q", i, sub.Lang, wantLang[i])
		}
		if !strings.HasSuffix(sub.URL, ".vtt") {
			t.Errorf("track %d URL = %q", i, sub.URL)
		}
	}
}

func TestKaaIslandSubtitleMetadata(t *testing.T) {
	page := `<astro-island props="` + kaaIslandProps + `" ssr client="only">`
	metas := kaaParseSubtitleMeta(page)
	if len(metas) != 4 {
		t.Fatalf("got %d subtitle metas, want 4: %+v", len(metas), metas)
	}
	wantLang := []string{"en", "th", "vi", "id"}
	wantLabel := []string{"English", "Thai", "Vietnamese", "Indonesian"}
	for i, m := range metas {
		sub := buildSubtitle(m.src, m.lang, m.name, "")
		if sub.Lang != wantLang[i] {
			t.Errorf("track %d lang = %q, want %q", i, sub.Lang, wantLang[i])
		}
		if sub.Label != wantLabel[i] {
			t.Errorf("track %d label = %q, want %q", i, sub.Label, wantLabel[i])
		}
		if strings.HasPrefix(sub.URL, "https:///") {
			t.Errorf("track %d URL not fixed: %q", i, sub.URL)
		}
		if strings.Contains(sub.URL, "preview-4koE-") {
			t.Errorf("thumbnails leaked into subtitle tracks")
		}
	}
}

// A page without island metadata (or with an island that does not parse)
// still yields subtitle tracks through the bare-URL scan.
func TestKaaPlayerSubtitlesFallback(t *testing.T) {
	f := newKaaFixture(t)
	p := newKaaTestProvider(f)
	txt := `//bl.krussdomi.com/less/349201_oc_modified.vtt`
	subs := p.playerSubtitles(`props="not json"`, txt)
	if len(subs) != 1 {
		t.Fatalf("got %d fallback tracks, want 1", len(subs))
	}
	if subs[0].URL != "https://bl.krussdomi.com/less/349201_oc_modified.vtt" {
		t.Errorf("fallback URL = %q", subs[0].URL)
	}
}
