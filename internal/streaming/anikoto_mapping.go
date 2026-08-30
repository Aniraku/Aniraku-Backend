package streaming

import (
	"encoding/json"
	"os"
	"sync"
)

// AnikotoMapping maps an AniList ID to an AnikotoTV show entry.
type AnikotoMapping struct {
	ShowID  string `json:"show_id"`
	Slug    string `json:"slug"`
	Title   string `json:"title,omitempty"`
}

var (
	anikotoMappingData map[string]AnikotoMapping
	anikotoMappingOnce sync.Once
	anikotoMappingMu   sync.RWMutex
)

// LoadAnikotoMapping loads the AniList→AnikotoTV mapping from disk.
func LoadAnikotoMapping(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var m map[string]AnikotoMapping
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	anikotoMappingMu.Lock()
	anikotoMappingData = m
	anikotoMappingMu.Unlock()
	return nil
}

// GetAnikotoMapping returns the cached mapping entry for an AniList ID, or nil.
func GetAnikotoMapping(anilistID string) *AnikotoMapping {
	anikotoMappingMu.RLock()
	defer anikotoMappingMu.RUnlock()
	if anikotoMappingData == nil {
		return nil
	}
	if entry, ok := anikotoMappingData[anilistID]; ok {
		return &entry
	}
	return nil
}

// SaveAnikotoMapping persists the mapping to disk as JSON.
func SaveAnikotoMapping(path string) error {
	anikotoMappingMu.RLock()
	defer anikotoMappingMu.RUnlock()
	if anikotoMappingData == nil {
		return nil
	}
	raw, err := json.MarshalIndent(anikotoMappingData, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0644)
}

// AddAnikotoMapping adds or updates a mapping entry in memory.
func AddAnikotoMapping(anilistID string, entry AnikotoMapping) {
	anikotoMappingMu.Lock()
	defer anikotoMappingMu.Unlock()
	if anikotoMappingData == nil {
		anikotoMappingData = make(map[string]AnikotoMapping)
	}
	anikotoMappingData[anilistID] = entry
}
