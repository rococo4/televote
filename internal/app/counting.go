package app

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/rococo4/televote/internal/entity"
	"github.com/rococo4/televote/internal/service/voting"
)

// CountClosedPoll может работать на нескольких репликах одновременно. Каждая ждёт,
// пока в PostgreSQL появится общее решение о закрытии, и только тогда ставит CLOSE в Kafka.
func (a *App) CountClosedPoll(ctx context.Context, id uuid.UUID) error {
	local, ok := a.lookup(id)
	if !ok {
		return voting.ErrPollNotFound
	}
	p := local.poll
	if time.Now().Before(p.ClosesAt) {
		return voting.ErrStillOpen
	}
	handlersDone, err := a.voting.Seal(id)
	if err != nil {
		return err
	}
	if local.joined {
		if err := a.drainWriter(ctx, local, handlersDone); err != nil {
			return err
		}
	} else {
		// У опросов, подсчитанных до введения учёта реплик, уже есть итог.
		// При перезапуске такой опрос можно пропустить.
		if final, err := a.counting.IsFinal(ctx, p); err != nil || final {
			return err
		}
	}
	ready, cutoff, err := a.closures.Decide(ctx, p)
	if err != nil {
		return err
	}
	if !ready {
		// Последний писатель сам начнёт закрытие. В общий срок проверим,
		// успел ли он закончить или нужно продолжить за него.
		timer := time.NewTimer(time.Until(cutoff))
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		ready, _, err = a.closures.Decide(ctx, p)
		if err != nil {
			return err
		}
		if !ready {
			// Если часы пода опережают PostgreSQL, finishPoll повторит попытку с задержкой.
			return fmt.Errorf("poll %s still waiting for close decision at %s", id, cutoff)
		}
	}
	return a.counting.Count(ctx, p)
}

func (a *App) drainWriter(ctx context.Context, local localPoll, handlersDone <-chan struct{}) error {
	// До одного и того же срока ждём завершения HTTP-запросов и подтверждений Kafka.
	// Даже если отправка зависнет на последней живой реплике, опрос можно будет закрыть.
	drainCtx, cancel := context.WithDeadline(ctx, local.drainDeadline)
	defer cancel()
	select {
	case <-drainCtx.Done():
		return ctx.Err()
	case <-handlersDone:
	}
	err := a.journal.WaitVotes(drainCtx, local.poll.ID)
	if drainCtx.Err() != nil {
		// Когда истёк срок ожидания, возвращаем nil, чтобы Decide мог закрыть опрос
		// по тайм-ауту. При остановке самого сервиса возвращаем ошибку отмены.
		return ctx.Err()
	}
	if err != nil {
		return err
	}
	return a.closures.Drain(ctx, local.poll, a.writerID)
}

func (a *App) finishPoll(ctx context.Context, p entity.Poll) {
	timer := time.NewTimer(time.Until(p.ClosesAt))
	defer timer.Stop()
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		err := a.CountClosedPoll(ctx, p.ID)
		if err == nil || ctx.Err() != nil {
			return
		}
		log.Printf("count poll %s: %v; retry in %s", p.ID, err, backoff)
		timer.Reset(backoff)
		backoff = min(backoff*2, time.Minute)
	}
}
