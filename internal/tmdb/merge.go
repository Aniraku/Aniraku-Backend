package tmdb

import (
	"regexp"
	"strings"
)

// Exported helpers for handlers (100% perfect mapping needs same validation as JS)

// IsGenericEpisodeLabel matches "Episode 1", "Ep 1", "EP1 · 1P" etc — considered not a real title
func IsGenericEpisodeLabel(s string) bool {
	t := strings.TrimSpace(s)
	if t == "" {
		return true
	}
	// from src/lib/tmdbEpisodes.js: ^(?:(?:episode|ep)\s*)?\d+(?:\s*(?:[·.-]\s*\d+\s*[ps]))?$
	if matched, _ := regexp.MatchString(`(?i)^(?:(?:episode|ep)\s*)?\d+(?:\s*(?:[·.-]\s*\d+\s*[ps]))?$`, t); matched {
		return true
	}
	return false
}

func IsPublishedTitle(s string) bool {
	t := strings.TrimSpace(s)
	if t == "" {
		return false
	}
	if IsGenericEpisodeLabel(t) {
		return false
	}
	if matched, _ := regexp.MatchString(`(?i)^(?:tba|tbd|untitled|unknown)$`, t); matched {
		return false
	}
	return true
}

func HasVerifiedTmdbThumbnail(url string) bool {
	u := strings.TrimSpace(url)
	if matched, _ := regexp.MatchString(`(?i)^https:\/\/image\.tmdb\.org\/t\/p\/(?:original|[wh]\d+)\/[A-Za-z0-9_-]+\.(?:jpg|jpeg|png|webp)$`, u); matched {
		return true
	}
	return false
}

func IsValidAniZipThumbnail(url string) bool {
	u := strings.TrimSpace(url)
	if u == "" {
		return false
	}
	if matched, _ := regexp.MatchString(`(?i)^https:\/\/[^\s]+$`, u); matched {
		return true
	}
	return false
}

// MergeEpisode returns final title/thumbnail picking the best of AniZip and TMDB.
// Bidirectional: TMDB verified wins, else AniZip verified, else fallback.
// This mirrors the contract in src/lib/tmdbEpisodes.js but server-side.
func MergeEpisode(anilistID, epNum int, aniZipTitle, aniZipThumb string, tmdb *EpisodeMetadata) (string, string) {
	// Title
	var finalTitle string
	tmdbTitle := ""
	if tmdb != nil {
		tmdbTitle = strings.TrimSpace(tmdb.Title)
	}
	aniTitle := strings.TrimSpace(aniZipTitle)

	tmdbValid := IsPublishedTitle(tmdbTitle)
	aniValid := IsPublishedTitle(aniTitle)

	if tmdbValid {
		finalTitle = tmdbTitle
	} else if aniValid {
		finalTitle = aniTitle
	} else {
		// Both invalid -> generic fallback (handlers will override with Episode N if needed)
		finalTitle = ""
	}

	// Thumbnail
	var finalThumb string
	tmdbThumb := ""
	if tmdb != nil && tmdb.Thumbnail != nil {
		tmdbThumb = strings.TrimSpace(*tmdb.Thumbnail)
	}
	aniThumb := strings.TrimSpace(aniZipThumb)
	tmdbThumbValid := HasVerifiedTmdbThumbnail(tmdbThumb)
	aniThumbValid := IsValidAniZipThumbnail(aniThumb)

	if tmdbThumbValid {
		finalThumb = tmdbThumb
	} else if aniThumbValid {
		finalThumb = aniThumb
	} else {
		finalThumb = ""
	}

	return finalTitle, finalThumb
}
