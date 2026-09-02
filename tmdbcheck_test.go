package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Aniraku/Aniraku-Backend/internal/tmdb"
)

func main() {
	client := &http.Client{Timeout: 60 * time.Second}
	token := "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI4ZDFjODFkZjA4ZjgwNTQ4ZmYxYjM1OTAxZTc4MzI5NyIsIm5iZiI6MTc4NzczNjc4NC4xOTEsInN1YiI6IjZhOGViMmQwMDg5YTRlNzUwYjExMjE3OSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.2AlmsyuPf-k8q78vPpyblBW9XxHJQih8ZL6CgCMIAL0"
	ctx := context.Background()
	nums := []int{1, 2, 3, 50, 100, 200}
	result, err := tmdb.ResolveEpisodes(ctx, client, token, 21, nums)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}
	fmt.Printf("OK: %d episodes, source=%s\n", len(result.Episodes), result.Source)
	for _, ep := range result.Episodes {
		thumb := ""
		if ep.Thumbnail != nil {
			thumb = *ep.Thumbnail
		}
		fmt.Printf("  ep %d: title=%q thumb=%s\n", ep.Number, ep.Title, thumb[:80])
	}
}
