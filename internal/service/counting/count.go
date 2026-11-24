package counting

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/rococo4/televote/internal/entity"
	"golang.org/x/sync/errgroup"
)

type Journal interface {
	TopicID(context.Context, entity.Poll) (uuid.UUID, error)
	WaitVotes(context.Context, uuid.UUID) error
	MarkClosed(context.Context, entity.Poll, int32) error
	ReadPartition(context.Context, entity.Poll, uuid.UUID, int32, func(entity.VoteSubmitted) error) (int64, error)
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
	// Bound memory across all polls: each reader holds one partition's voter keys.
	slots chan struct{}
}

func New(journal Journal, results Results) *Service {
	return &Service{journal: journal, results: results, slots: make(chan struct{}, 4)}
}

// Prepare binds the published configuration to the broker's topic UUID.
func (s *Service) Prepare(ctx context.Context, p entity.Poll) error {
	id, err := s.journal.TopicID(ctx, p)
	if err != nil {
		return err
	}
	return s.results.Bind(ctx, p, id)
}

// Count is called after the API seals the poll and drains registered handlers.
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
		// Closed polls from before counting was deployed have no saved topic UUID.
		// Adopt their existing journal once, then check its identity on every retry.
		if err := s.results.Bind(ctx, p, currentID); err != nil {
			return err
		}
		topicID = currentID
	} else if topicID != currentID {
		return fmt.Errorf("Kafka topic %s has been replaced", p.Topic)
	}
	if err := s.journal.WaitVotes(ctx, p.ID); err != nil {
		return err
	}
	for partition := range int32(p.Partitions) {
		if !done[partition] {
			if err := s.journal.MarkClosed(ctx, p, partition); err != nil {
				return fmt.Errorf("close partition %d: %w", partition, err)
			}
		}
	}

	group, workerCtx := errgroup.WithContext(ctx)
	for partition := range int32(p.Partitions) {
		if done[partition] {
			continue
		}
		select {
		case s.slots <- struct{}{}:
			group.Go(func() error {
				defer func() { <-s.slots }()
				result, err := s.countPartition(workerCtx, p, topicID, partition)
				if err != nil {
					return fmt.Errorf("count partition %d: %w", partition, err)
				}
				return s.results.SavePartition(workerCtx, p, result)
			})
		case <-workerCtx.Done():
			if err := group.Wait(); err != nil {
				return err
			}
			return workerCtx.Err()
		}
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return s.results.Finalize(ctx, p)
}

func (s *Service) countPartition(ctx context.Context, p entity.Poll, topicID uuid.UUID, partition int32) (entity.PartitionResult, error) {
	last := make(map[[32]byte][]int)
	end, err := s.journal.ReadPartition(ctx, p, topicID, partition, func(vote entity.VoteSubmitted) error {
		if vote.PollID == p.ID && vote.Version == p.Version && p.CheckWindow(vote.ReceivedAt) == nil &&
			p.ValidateBallot(entity.Ballot{Choices: vote.Choices}) == nil {
			last[vote.VoterKey] = vote.Choices
		}
		return nil
	})
	if err != nil {
		return entity.PartitionResult{}, err
	}
	counts := make(map[int]int64, len(p.Choices))
	for id := range p.Choices {
		counts[id] = 0
	}
	for _, choices := range last {
		for _, id := range choices {
			counts[id]++
		}
	}
	return entity.PartitionResult{Partition: partition, EndOffset: end, Ballots: int64(len(last)), Counts: counts}, nil
}
