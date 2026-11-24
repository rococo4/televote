package voting

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/rococo4/televote/internal/entity"
)

var ErrStillOpen = errors.New("poll deadline has not passed")

// Admission tracks a complete request through validation and the producer ACK.
type Admission struct {
	poll       entity.Poll
	receivedAt time.Time
	done       func()
}

// Finish releases the registration. Call it once, even when the body is invalid.
func (a *Admission) Finish() { a.done() }

func (s *Service) Begin(pollID uuid.UUID) (*Admission, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if s.draining {
		return nil, ErrUnavailable
	}
	receivedAt := s.now().UTC()
	intake, ok := s.polls[pollID]
	if !ok {
		return nil, ErrPollNotFound
	}
	if intake.sealed != nil {
		return nil, entity.ErrClosed
	}
	if err := intake.poll.CheckWindow(receivedAt); err != nil {
		return nil, err
	}
	intake.inFlight.Add(1)
	return &Admission{poll: intake.poll, receivedAt: receivedAt, done: intake.inFlight.Done}, nil
}

// Seal stops admissions for this poll, then waits for registered handlers.
// The producer separately waits for any delivery whose HTTP request timed out.
func (s *Service) Seal(ctx context.Context, id uuid.UUID) (entity.Poll, error) {
	s.gate.Lock()
	intake := s.polls[id]
	if intake == nil {
		s.gate.Unlock()
		return entity.Poll{}, ErrPollNotFound
	}
	if intake.sealed == nil {
		if s.now().Before(intake.poll.ClosesAt) {
			s.gate.Unlock()
			return entity.Poll{}, ErrStillOpen
		}
		intake.sealed = make(chan struct{})
		go func() {
			intake.inFlight.Wait()
			close(intake.sealed)
		}()
	}
	s.gate.Unlock()
	select {
	case <-intake.sealed:
		return intake.poll, nil
	case <-ctx.Done():
		return entity.Poll{}, ctx.Err()
	}
}

// Drain stops new admissions and waits for all registered handlers to finish.
func (s *Service) Drain() {
	s.gate.Lock()
	s.draining = true
	s.gate.Unlock()
	for _, intake := range s.polls {
		intake.inFlight.Wait()
	}
}
