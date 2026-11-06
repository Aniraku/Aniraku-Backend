package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/streaming"
)

func main() {
	id := 1642
	ep := 1
	if len(os.Args) > 1 {
		id, _ = strconv.Atoi(os.Args[1])
	}
	if len(os.Args) > 2 {
		ep, _ = strconv.Atoi(os.Args[2])
	}
	provider := os.Args[3]
	var sr *streaming.SourceResult
	var err error
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	switch provider {
	case "animex":
		apx := streaming.NewAnimeXProvider(zerolog.Nop(), "")
		sr, err = apx.FindEpisodeSource(ctx, fmt.Sprintf("%d", id), ep, "sub")
	case "anipm":
		apm := streaming.NewAniPMProvider(zerolog.Nop(), "", "")
		sr, err = apm.FindEpisodeSource(ctx, fmt.Sprintf("%d", id), ep, "sub")
	}
	if err != nil {
		fmt.Println("ERR:", err)
		os.Exit(1)
	}
	if sr == nil || len(sr.Sources) == 0 {
		fmt.Println("NIL: no sources (silent drop path)")
		os.Exit(2)
	}
	for i, s := range sr.Sources {
		name := ""
		if i < len(sr.ServerNames) {
			name = sr.ServerNames[i]
		}
		u := s.URL
		if len(u) > 55 {
			u = u[:55]
		}
		fmt.Printf("OK server=%s type=%s url=%s\n", name, s.Type, u)
	}
}
