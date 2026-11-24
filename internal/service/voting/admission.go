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
	poll, ok := s.polls[pollID]
	if !ok {
		return nil, ErrPollNotFound
	}
	if err := poll.CheckWindow(receivedAt); err != nil {
		return nil, err
	}
	s.inFlight.Add(1)
	return &Admission{poll: poll, receivedAt: receivedAt, done: s.inFlight.Done}, nil
}

// Drain stops new admissions and waits for all registered handlers to finish.
func (s *Service) Drain() {
	s.gate.Lock()
	s.draining = true
	s.gate.Unlock()
	s.inFlight.Wait()
}
