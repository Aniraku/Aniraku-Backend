package v1

import "strings"

// Dual-audio track selection for the HLS rewrite.
//
// Some upstream manifests are dual-audio — they carry BOTH the dub (English)
// and the sub/original (Japanese) rendition in one master — and serve that
// SAME manifest for a sub request and a dub request (observed on Sora /
// krussdomi for anilist 130298: type=sub and type=dub return the byte-identical
// master; the official Kickassanime site shows the same wrong-track bug).
// The player-side selection then lands on the wrong track, so dub plays SUB
// and sub plays DUB.
//
// The fix lives in the proxy's playlist rewrite, because the proxy is the
// only backend-owned step on the playback path: every proxied master carries
// an `al` (audio language) parameter set from the request's lang at wrap
// time, and the rewrite strips every AUDIO rendition that does not match it.
// After the strip only the correct track exists, so no player-side selection
// logic — default flags, track order, frontend heuristics — can pick the
// wrong one. Masters without dual audio (no match, or a single rendition)
// are returned byte-identical.

// audioLangMatches reports whether an AUDIO rendition serves the requested
// track language: sub → Japanese (the original audio), dub → English.
// Language codes are prefix-matched so "eng"/"ja"/"jpn" all match.
func audioLangMatches(al, language, name string) bool {
	language = strings.ToLower(language)
	name = strings.ToLower(name)
	switch al {
	case "sub":
		return strings.HasPrefix(language, "ja") || strings.HasPrefix(language, "jp") ||
			strings.Contains(name, "japanese")
	case "dub":
		return strings.HasPrefix(language, "en") || strings.Contains(name, "english")
	}
	return false
}

// splitM3U8Attrs splits an HLS attribute list on commas, ignoring commas
// inside quoted values (NAME="English, US" must stay one attribute).
func splitM3U8Attrs(s string) []string {
	var (
		parts   []string
		cur     strings.Builder
		inQuote bool
	)
	for _, r := range s {
		switch {
		case r == '"':
			inQuote = !inQuote
			cur.WriteRune(r)
		case r == ',' && !inQuote:
			parts = append(parts, cur.String())
			cur.Reset()
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		parts = append(parts, cur.String())
	}
	return parts
}

// m3u8AttrValue returns the unquoted value of key in a parsed attribute list
// ("" when absent).
func m3u8AttrValue(attrs []string, key string) string {
	prefix := key + "="
	for _, a := range attrs {
		if strings.HasPrefix(a, prefix) {
			return strings.Trim(strings.TrimPrefix(a, prefix), `"`)
		}
	}
	return ""
}

// stripAudioRenditions removes #EXT-X-MEDIA TYPE=AUDIO renditions that do not
// match al (audio language) from a master playlist. Conservative rules:
//
//   - Only runs for al=sub/al=dub; any other value returns content unchanged.
//   - Only acts on groups where at least one rendition matches AND at least
//     one does not (nothing to strip → unchanged).
//   - If no rendition in a group matches, the group is left untouched —
//     there is nothing correct to keep, so current behavior is preserved.
//   - The surviving rendition of every touched group gets DEFAULT=YES so a
//     standards-compliant player lands on it even without explicit selection.
//   - TYPE=SUBTITLES / TYPE=VIDEO renditions are never touched.
//
// The returned content is the stripped playlist (with original line order and
// formatting preserved); callers rewrite URIs afterwards.
func stripAudioRenditions(content, al string) string {
	if al != "sub" && al != "dub" {
		return content
	}
	lines := strings.Split(content, "\n")

	type rendition struct {
		idx     int
		matched bool
	}
	byGroup := map[string][]rendition{}
	audioAttrs := map[int][]string{} // line idx -> parsed attrs (kept lines)
	for i, ln := range lines {
		t := strings.TrimSpace(ln)
		if !strings.HasPrefix(t, "#EXT-X-MEDIA:") {
			continue
		}
		attrs := splitM3U8Attrs(strings.TrimPrefix(t, "#EXT-X-MEDIA:"))
		if m3u8AttrValue(attrs, "TYPE") != "AUDIO" {
			continue
		}
		group := m3u8AttrValue(attrs, "GROUP-ID")
		matched := audioLangMatches(al,
			m3u8AttrValue(attrs, "LANGUAGE"),
			m3u8AttrValue(attrs, "NAME"))
		byGroup[group] = append(byGroup[group], rendition{idx: i, matched: matched})
		audioAttrs[i] = attrs
	}
	if len(byGroup) == 0 {
		return content
	}

	drop := map[int]bool{}
	firstKept := map[int]bool{}        // line idx of the group's surviving DEFAULT=YES rendition
	touchedGroups := map[string]bool{} // groups where at least one rendition was dropped
	for g, group := range byGroup {
		if len(group) < 2 {
			continue // a single rendition is already unambiguous
		}
		hasMatch := false
		for _, r := range group {
			if r.matched {
				hasMatch = true
				break
			}
		}
		if !hasMatch {
			continue // no correct track to keep: leave the group alone
		}
		kept := 0
		for _, r := range group {
			if !r.matched {
				drop[r.idx] = true
				touchedGroups[g] = true
				continue
			}
			if kept == 0 {
				firstKept[r.idx] = true
			}
			kept++
		}
	}
	if len(drop) == 0 {
		return content
	}

	out := make([]string, 0, len(lines))
	for i, ln := range lines {
		if drop[i] {
			continue
		}
		if attrs, ok := audioAttrs[i]; ok {
			if touchedGroups[m3u8AttrValue(attrs, "GROUP-ID")] {
				if firstKept[i] {
					ln = rebuildMediaLine(attrs, "YES")
				} else {
					ln = rebuildMediaLine(attrs, "")
				}
			}
		}
		out = append(out, ln)
	}
	return strings.Join(out, "\n")
}

// rebuildMediaLine re-serializes an #EXT-X-MEDIA line from its parsed
// attributes, replacing any DEFAULT= flag: value "YES" sets DEFAULT=YES
// (appended at the end), "" removes the flag entirely (defaults to NO).
func rebuildMediaLine(attrs []string, defaultVal string) string {
	out := make([]string, 0, len(attrs)+1)
	for _, a := range attrs {
		if strings.HasPrefix(a, "DEFAULT=") {
			continue
		}
		out = append(out, a)
	}
	if defaultVal != "" {
		out = append(out, "DEFAULT="+defaultVal)
	}
	return "#EXT-X-MEDIA:" + strings.Join(out, ",")
}
