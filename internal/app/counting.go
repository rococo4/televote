package app

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rococo4/televote/internal/entity"
	"github.com/rococo4/televote/internal/service/voting"
)

// RunCounting finishes a fixed snapshot. ServePolls is the long-running scheduler.
func (h *Handler) RunCounting(ctx context.Context) {
	var jobs sync.WaitGroup
	h.mu.RLock()
	for _, local := range h.polls {
		jobs.Go(func() { h.finishPoll(ctx, local.poll) })
	}
	h.mu.RUnlock()
	jobs.Wait()
}

// CountClosedPoll may be called concurrently on multiple replicas. Every
// replica must observe the same durable decision before appending a marker.
func (h *Handler) CountClosedPoll(ctx context.Context, id uuid.UUID) error {
	local, ok := h.lookup(id)
	if !ok {
		return voting.ErrPollNotFound
	}
	p := local.poll
	if time.Now().Before(p.ClosesAt) {
		return voting.ErrStillOpen
	}
	handlersDone, err := h.voting.Seal(id)
	if err != nil {
		return err
	}
	if local.joined {
		if err := h.drainWriter(ctx, local, handlersDone); err != nil {
			return err
		}
	} else {
		// On restart an older release may already have finalized this poll.
		// Preserve that result without inventing a retrospective timeout.
		if final, err := h.counting.IsFinal(ctx, p); err != nil || final {
			return err
		}
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, err := h.closures.Decide(ctx, p)
		if err != nil {
			return err
		}
		if ready {
			return h.counting.Count(ctx, p)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (h *Handler) drainWriter(ctx context.Context, local localPoll, handlersDone <-chan struct{}) error {
	// Apply one absolute budget to both HTTP handlers and outstanding Kafka
	// callbacks. A stuck send on the last surviving scheduler must not stall it.
	drainCtx, cancel := context.WithDeadline(ctx, local.drainDeadline)
	defer cancel()
	select {
	case <-drainCtx.Done():
		return ctx.Err()
	case <-handlersDone:
	}
	err := h.journal.WaitVotes(drainCtx, local.poll.ID)
	if drainCtx.Err() != nil {
		// The grace deadline permits a timeout decision. Caller cancellation does not.
		return ctx.Err()
	}
	if err != nil {
		return err
	}
	return h.closures.Drain(ctx, local.poll, h.writerID)
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
