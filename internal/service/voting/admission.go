package voting

import (
	"time"

	"github.com/google/uuid"
	"github.com/rococo4/televote/internal/entity"
)

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

// Seal stops admissions permanently. The returned channel closes when every
// admitted handler has finished and no more sends can be queued for this poll.
func (s *Service) Seal(id uuid.UUID) (<-chan struct{}, error) {
	s.gate.Lock()
	defer s.gate.Unlock()
	intake := s.polls[id]
	if intake == nil {
		return nil, ErrPollNotFound
	}
	if intake.sealed == nil {
		intake.sealed = make(chan struct{})
		go func() {
			intake.inFlight.Wait()
			close(intake.sealed)
		}()
	}
	return intake.sealed, nil
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
