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

func (a *App) loadForVote(ctx context.Context, id uuid.UUID) error {
	_, err, _ := a.loading.Do("lookup:"+id.String(), func() (any, error) {
		if _, ok := a.lookup(id); ok {
			return nil, nil
		}
		p, err := a.source.FindPublished(ctx, id)
		if err != nil {
			return nil, err
		}
		return nil, a.registerPoll(ctx, p)
	})
	if errors.Is(err, entity.ErrPollNotFound) {
		return voting.ErrPollNotFound
	}
	if err != nil {
		return errors.Join(voting.ErrUnavailable, err)
	}
	return nil
}

func (a *App) lookup(id uuid.UUID) (localPoll, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	local, ok := a.polls[id]
	return local, ok
}

// Публикация опроса, фоновая проверка базы и первый голос могут одновременно
// запустить регистрацию. Для одного опроса реплика проходит её только один раз.
func (a *App) registerPoll(ctx context.Context, p entity.Poll) error {
	if _, ok := a.lookup(p.ID); ok {
		return nil
	}
	_, err, _ := a.loading.Do(p.ID.String(), func() (any, error) {
		if _, ok := a.lookup(p.ID); ok {
			return nil, nil
		}
		local := localPoll{poll: p}
		if time.Now().Before(p.ClosesAt) {
			if err := a.counting.Prepare(ctx, p); err != nil {
				return nil, err
			}
			var err error
			local.joined, local.drainDeadline, err = a.closures.Join(ctx, p, a.writerID)
			if err != nil {
				return nil, err
			}
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		if err := a.voting.Register(p, local.joined); err != nil {
			return nil, err
		}
		a.polls[p.ID] = local
		if a.startJob != nil {
			a.startJob(p)
		}
		return nil, nil
	})
	return err
}

// ServePolls планирует закрытие известных опросов и ищет новые в PostgreSQL.
// Так другая реплика сможет подвести итог, даже если создавшая опрос реплика
// остановилась, а зрители больше не прислали ни одного голоса.
func (a *App) ServePolls(ctx context.Context) {
	var jobs sync.WaitGroup
	a.mu.Lock()
	a.startJob = func(p entity.Poll) { jobs.Go(func() { a.finishPoll(ctx, p) }) }
	for _, local := range a.polls {
		a.startJob(local.poll)
	}
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.startJob = nil
		a.mu.Unlock()
		jobs.Wait()
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			published, err := a.source.LoadUnfinished(ctx)
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("discover polls: %v", err)
				}
				continue
			}
			for _, p := range published {
				if err := a.registerPoll(ctx, p); err != nil && ctx.Err() == nil {
					log.Printf("register poll %s: %v", p.ID, err)
				}
			}
		}
	}
}
