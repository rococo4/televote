package voting

import (
	"context"
	"errors"
	"maps"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rococo4/televote/internal/entity"
)

type Publisher interface {
	Publish(context.Context, entity.Poll, entity.VoteSubmitted) error
}

var (
	ErrPollNotFound = errors.New("poll not found")
	ErrUnavailable  = errors.New("vote intake is unavailable")
	ErrStillOpen    = errors.New("poll deadline has not passed")
)

type Service struct {
	polls     map[uuid.UUID]*pollIntake
	publisher Publisher
	now       func() time.Time

	gate     sync.RWMutex
	draining bool
}

type pollIntake struct {
	poll     entity.Poll
	inFlight sync.WaitGroup
	sealed   chan struct{}
}

func New(polls []entity.Poll, publisher Publisher, now func() time.Time) *Service {
	snapshot := make(map[uuid.UUID]*pollIntake, len(polls))
	for _, poll := range polls {
		poll.Choices = maps.Clone(poll.Choices)
		snapshot[poll.ID] = &pollIntake{poll: poll}
	}
	return &Service{polls: snapshot, publisher: publisher, now: now}
}

func (s *Service) Submit(ctx context.Context, a *Admission, token [16]byte, ballot entity.Ballot) error {
	if err := a.poll.ValidateBallot(ballot); err != nil {
		return err
	}
	event := entity.VoteSubmitted{
		PollID:     a.poll.ID,
		Version:    a.poll.Version,
		ReceivedAt: a.receivedAt,
		VoterKey:   entity.VoterKey(a.poll.ID, token),
		Choices:    ballot.Choices,
	}
	// A disconnected browser can retry. Finish the registered publication under
	// a server-owned deadline so shutdown cannot overtake it.
	publishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := s.publisher.Publish(publishCtx, a.poll, event); err != nil {
		return errors.Join(ErrUnavailable, err)
	}
	return nil
}
