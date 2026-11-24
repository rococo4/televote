package app

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/rococo4/televote/internal/entity"
)

// RunCounting closes published polls at their deadlines and resumes closed polls.
// It returns when all known results are final or the application is stopping.
func (h *Handler) RunCounting(ctx context.Context) {
	var jobs sync.WaitGroup
	for _, p := range h.polls {
		jobs.Go(func() { h.finishPoll(ctx, p) })
	}
	jobs.Wait()
}

func (h *Handler) finishPoll(ctx context.Context, p entity.Poll) {
	timer := time.NewTimer(time.Until(p.ClosesAt))
	defer timer.Stop()
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		err := h.CountClosedPoll(ctx, p.ID)
		if err == nil || ctx.Err() != nil {
			return
		}
		log.Printf("count poll %s version %d: %v; retry in %s", p.ID, p.Version, err, backoff)
		timer.Reset(backoff)
		backoff = min(backoff*2, time.Minute)
	}
}
