package app

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rococo4/televote/internal/entity"
	"github.com/rococo4/televote/internal/service/voting"
)

func (h *Handler) loadForVote(ctx context.Context, id uuid.UUID) error {
	_, err, _ := h.loading.Do("lookup:"+id.String(), func() (any, error) {
		if _, ok := h.lookup(id); ok {
			return nil, nil
		}
		p, err := h.source.FindPublished(ctx, id)
		if err != nil {
			return nil, err
		}
		return nil, h.registerPoll(ctx, p)
	})
	if errors.Is(err, entity.ErrPollNotFound) {
		return voting.ErrPollNotFound
	}
	if err != nil {
		return errors.Join(voting.ErrUnavailable, err)
	}
	return nil
}

func (h *Handler) lookup(id uuid.UUID) (localPoll, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	local, ok := h.polls[id]
	return local, ok
}

// Publication, discovery and the first vote on a cold replica can race. Only
// one of them registers this writer and exposes the poll to local requests.
func (h *Handler) registerPoll(ctx context.Context, p entity.Poll) error {
	if _, ok := h.lookup(p.ID); ok {
		return nil
	}
	_, err, _ := h.loading.Do(p.ID.String(), func() (any, error) {
		if _, ok := h.lookup(p.ID); ok {
			return nil, nil
		}
		local := localPoll{poll: p}
		if time.Now().Before(p.ClosesAt) {
			if err := h.counting.Prepare(ctx, p); err != nil {
				return nil, err
			}
			var err error
			local.joined, local.drainDeadline, err = h.closures.Join(ctx, p, h.writerID)
			if err != nil {
				return nil, err
			}
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		if err := h.voting.Register(p, local.joined); err != nil {
			return nil, err
		}
		h.polls[p.ID] = local
		if h.startJob != nil {
			h.startJob(p)
		}
		return nil, nil
	})
	return err
}

// ServePolls schedules both the initial snapshot and newly published polls.
// Discovery lets another replica finish a poll even if its creator disappears
// and nobody votes on the remaining replicas.
func (h *Handler) ServePolls(ctx context.Context) {
	var jobs sync.WaitGroup
	h.mu.Lock()
	h.startJob = func(p entity.Poll) { jobs.Go(func() { h.finishPoll(ctx, p) }) }
	for _, local := range h.polls {
		h.startJob(local.poll)
	}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		h.startJob = nil
		h.mu.Unlock()
		jobs.Wait()
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			published, err := h.source.LoadUnfinished(ctx)
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("discover polls: %v", err)
				}
				continue
			}
			for _, p := range published {
				if err := h.registerPoll(ctx, p); err != nil && ctx.Err() == nil {
					log.Printf("register poll %s: %v", p.ID, err)
				}
			}
		}
	}
}
