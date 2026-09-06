package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/streaming"
)

func main() {
	output := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	log := zerolog.New(output).With().Timestamp().Logger()
	_ = streaming.LoadAnikotoMapping("") // bundled mapping
	p := streaming.NewAnikotoProvider(log)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ids := os.Args[1:]
	if len(ids) == 0 {
		ids = []string{"5114", "16498", "1"}
	}
	for _, id := range ids {
		start := time.Now()
		res, err := p.FindEpisodeSource(ctx, id, 1, "sub")
		fmt.Printf("=== anilist %s -> err=%v (took %s)\n", id, err, time.Since(start).Round(time.Millisecond))
		if res != nil {
			for _, s := range res.Sources {
				fmt.Printf("    source: %s (%s) subs=%d\n", s.URL, s.Verification, len(s.Subtitles))
			}
			for _, d := range res.Downloads {
				fmt.Printf("    download: %s (%s)\n", d.URL, d.Label)
			}
		}
	}
}
