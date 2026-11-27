package polls

import (
	"context"

	"github.com/google/uuid"
	"github.com/rococo4/televote/internal/entity"
)

type store interface {
	Draft(ctx context.Context, spec entity.CreatePoll, partitions int) (poll entity.Poll, published bool, err error)
	Publish(ctx context.Context, poll entity.Poll, topicID uuid.UUID) error
	Results(ctx context.Context, id uuid.UUID) (entity.PollReport, error)
}

type topics interface {
	EnsureTopic(context.Context, entity.Poll) (uuid.UUID, error)
}

type Service struct {
	store      store
	topics     topics
	partitions int
	activate   func(context.Context, entity.Poll) error
}

func New(store store, topics topics, partitions int, activate func(context.Context, entity.Poll) error) *Service {
	return &Service{store: store, topics: topics, partitions: partitions, activate: activate}
}

func (s *Service) Create(ctx context.Context, spec entity.CreatePoll) (entity.Poll, error) {
	spec, err := spec.Normalize()
	if err != nil {
		return entity.Poll{}, err
	}
	if spec.ID == uuid.Nil {
		spec.ID, err = uuid.NewRandom()
		if err != nil {
			return entity.Poll{}, err
		}
	}
	poll, published, err := s.store.Draft(ctx, spec, s.partitions)
	if err != nil {
		return entity.Poll{}, err
	}
	if !published {
		topicID, err := s.topics.EnsureTopic(ctx, poll)
		if err != nil {
			return entity.Poll{}, err
		}
		if err := s.store.Publish(ctx, poll, topicID); err != nil {
			return entity.Poll{}, err
		}
	}
	// При повторном запросе опрос уже может быть опубликован в базе,
	// хотя эта реплика ещё не успела открыть его для голосования.
	if err := s.activate(ctx, poll); err != nil {
		return entity.Poll{}, err
	}
	return poll, nil
}

func (s *Service) Results(ctx context.Context, id uuid.UUID) (entity.PollReport, error) {
	return s.store.Results(ctx, id)
}
