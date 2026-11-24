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
)

type Service struct {
	polls     map[uuid.UUID]entity.Poll
	publisher Publisher
	now       func() time.Time

	gate     sync.RWMutex
	draining bool
	inFlight sync.WaitGroup
}

func New(polls []entity.Poll, publisher Publisher, now func() time.Time) *Service {
	snapshot := make(map[uuid.UUID]entity.Poll, len(polls))
	for _, poll := range polls {
		poll.Choices = maps.Clone(poll.Choices)
		snapshot[poll.ID] = poll
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
