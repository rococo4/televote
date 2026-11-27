package counting

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/rococo4/televote/internal/entity"
)

// GroupCallbacks позволяет читателю узнать, какие итоги уже записаны в PostgreSQL,
// и начать подсчёт одной партиции с нуля. Если Kafka отзывает партицию,
// читатель отбрасывает накопленные голоса. Итог сохраняется на первом CLOSE.
type GroupCallbacks struct {
	Progress func(context.Context) (bool, map[int32]bool, error)
	Start    func(partition int32) (visit func(entity.VoteSubmitted) error, save func(context.Context, int64) error)
}

type Journal interface {
	TopicID(context.Context, entity.Poll) (uuid.UUID, error)
	MarkClosed(context.Context, entity.Poll, int32) error
	ConsumePoll(context.Context, entity.Poll, uuid.UUID, GroupCallbacks) error
}

type Results interface {
	Bind(context.Context, entity.Poll, uuid.UUID) error
	BoundTopic(context.Context, entity.Poll) (uuid.UUID, bool, error)
	Progress(context.Context, entity.Poll) (bool, map[int32]bool, error)
	SavePartition(context.Context, entity.Poll, entity.PartitionResult) error
	Finalize(context.Context, entity.Poll) error
}

type Service struct {
	journal Journal
	results Results
	// На каждый занятый слот приходится одна партиция, голоса которой хранятся в памяти.
	slots chan struct{}
}

func New(journal Journal, results Results) *Service {
	return &Service{journal: journal, results: results, slots: make(chan struct{}, 4)}
}

// Prepare сохраняет UUID топика, чтобы при подсчёте обнаружить его замену.
func (s *Service) Prepare(ctx context.Context, p entity.Poll) error {
	id, err := s.journal.TopicID(ctx, p)
	if err != nil {
		return err
	}
	return s.results.Bind(ctx, p, id)
}

func (s *Service) IsFinal(ctx context.Context, p entity.Poll) (bool, error) {
	final, _, err := s.results.Progress(ctx, p)
	return final, err
}

// Count ставит маркеры CLOSE и запускает подсчёт. К моменту вызова решение
// о закрытии опроса уже должно быть записано в PostgreSQL.
func (s *Service) Count(ctx context.Context, p entity.Poll) error {
	final, done, err := s.results.Progress(ctx, p)
	if err != nil || final {
		return err
	}
	if len(done) == p.Partitions {
		return s.results.Finalize(ctx, p)
	}
	topicID, bound, err := s.results.BoundTopic(ctx, p)
	if err != nil {
		return err
	}
	currentID, err := s.journal.TopicID(ctx, p)
	if err != nil {
		return err
	}
	if !bound {
		// У старых опросов UUID топика ещё не сохранялся. Записываем его сейчас,
		// чтобы при повторном подсчёте заметить замену топика под тем же именем.
		if err := s.results.Bind(ctx, p, currentID); err != nil {
			return err
		}
		topicID = currentID
	} else if topicID != currentID {
		return fmt.Errorf("Kafka topic %s has been replaced", p.Topic)
	}
	for partition := range int32(p.Partitions) {
		if !done[partition] {
			if err := s.journal.MarkClosed(ctx, p, partition); err != nil {
				return fmt.Errorf("close partition %d: %w", partition, err)
			}
		}
	}

	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	case <-ctx.Done():
		return ctx.Err()
	}
	callbacks := GroupCallbacks{
		Progress: func(ctx context.Context) (bool, map[int32]bool, error) {
			return s.results.Progress(ctx, p)
		},
		Start: func(partition int32) (func(entity.VoteSubmitted) error, func(context.Context, int64) error) {
			return s.startPartition(p, partition)
		},
	}
	if err := s.journal.ConsumePoll(ctx, p, topicID, callbacks); err != nil {
		return err
	}
	return s.results.Finalize(ctx, p)
}

func (s *Service) startPartition(p entity.Poll, partition int32) (func(entity.VoteSubmitted) error, func(context.Context, int64) error) {
	last := make(map[[32]byte][]int)
	visit := func(vote entity.VoteSubmitted) error {
		if vote.PollID == p.ID && p.CheckWindow(vote.ReceivedAt) == nil &&
			p.ValidateBallot(entity.Ballot{Choices: vote.Choices}) == nil {
			last[vote.VoterKey] = vote.Choices
		}
		return nil
	}
	save := func(ctx context.Context, end int64) error {
		counts := make(map[int]int64, len(p.Choices))
		for id := range p.Choices {
			counts[id] = 0
		}
		for _, choices := range last {
			for _, id := range choices {
				counts[id]++
			}
		}
		return s.results.SavePartition(ctx, p, entity.PartitionResult{
			Partition: partition, EndOffset: end, Ballots: int64(len(last)), Counts: counts,
		})
	}
	return visit, save
}
