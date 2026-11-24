package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/rococo4/televote/internal/entity"
)

// TopicID binds the poll to a specific Kafka topic incarnation and partition set.
func (p *Producer) TopicID(ctx context.Context, poll entity.Poll) (uuid.UUID, error) {
	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = false
	req.Topics = []kmsg.MetadataRequestTopic{{Topic: &poll.Topic}}
	resp, err := req.RequestWith(ctx, p.client)
	if err != nil {
		return uuid.Nil, err
	}
	if len(resp.Topics) != 1 {
		return uuid.Nil, fmt.Errorf("expected metadata for topic %q, got %d topics", poll.Topic, len(resp.Topics))
	}
	topic := resp.Topics[0]
	if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
		return uuid.Nil, fmt.Errorf("metadata for %q: %w", poll.Topic, err)
	}
	if topic.Topic == nil || *topic.Topic != poll.Topic || topic.TopicID == uuid.Nil {
		return uuid.Nil, fmt.Errorf("missing Kafka identity for %q", poll.Topic)
	}
	if poll.Partitions < 1 || len(topic.Partitions) != poll.Partitions {
		return uuid.Nil, fmt.Errorf("topic %q has %d partitions, poll specifies %d", poll.Topic, len(topic.Partitions), poll.Partitions)
	}
	seen := make([]bool, poll.Partitions)
	for _, part := range topic.Partitions {
		if err := kerr.ErrorForCode(part.ErrorCode); err != nil {
			return uuid.Nil, fmt.Errorf("metadata for %s[%d]: %w", poll.Topic, part.Partition, err)
		}
		if part.Partition < 0 || int(part.Partition) >= len(seen) || seen[part.Partition] {
			return uuid.Nil, fmt.Errorf("unexpected partition %s[%d]", poll.Topic, part.Partition)
		}
		seen[part.Partition] = true
	}
	return uuid.UUID(topic.TopicID), nil
}

func (p *Producer) MarkClosed(ctx context.Context, poll entity.Poll, partition int32) error {
	if partition < 0 || int(partition) >= poll.Partitions {
		return fmt.Errorf("partition %d outside poll %s", partition, poll.ID)
	}
	value, err := json.Marshal(eventHeader{Type: "CLOSE", PollID: poll.ID, Version: poll.Version})
	if err != nil {
		return err
	}
	record := &kgo.Record{Topic: poll.Topic, Partition: partition, Value: value}
	return p.client.ProduceSync(ctx, record).FirstErr()
}

// ReadPartition replays a dedicated poll log, whose initial offsets are zero.
// Each task owns its client and cursor; no consumer group commits are involved.
func (p *Producer) ReadPartition(ctx context.Context, poll entity.Poll, topicID uuid.UUID, partition int32, visit func(entity.VoteSubmitted) error) (int64, error) {
	if topicID == uuid.Nil || partition < 0 || int(partition) >= poll.Partitions {
		return 0, errors.New("invalid topic identity or partition for replay")
	}
	reader, err := kgo.NewClient(
		kgo.SeedBrokers(p.brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{poll.Topic: {partition: kgo.NewOffset().At(0)}}),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()),
	)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	var next int64
	for {
		batch := reader.PollFetches(ctx)
		if errs := batch.Errors(); len(errs) != 0 {
			return 0, errs[0].Err
		}
		for _, fetch := range batch {
			for _, topic := range fetch.Topics {
				if topic.Topic != poll.Topic || topic.TopicID != topicID {
					return 0, fmt.Errorf("topic identity changed during replay of %s[%d]", poll.Topic, partition)
				}
				if len(topic.Partitions) != 1 || topic.Partitions[0].Partition != partition {
					return 0, fmt.Errorf("unexpected assignment for %s[%d]", poll.Topic, partition)
				}
				part := topic.Partitions[0]
				if part.LogStartOffset > next {
					return 0, fmt.Errorf("lost range [%d, %d) in %s[%d]", next, part.LogStartOffset, poll.Topic, partition)
				}
				for _, record := range part.Records {
					// This dedicated delete-only topic has no transactions or
					// control batches. A hole here means input is missing.
					if record.Offset != next {
						return 0, fmt.Errorf("expected offset %d, got %d in %s[%d]", next, record.Offset, poll.Topic, partition)
					}
					closed, err := readEvent(poll, record, visit)
					if err != nil {
						return 0, fmt.Errorf("%s[%d] at offset %d: %w", poll.Topic, partition, record.Offset, err)
					}
					if closed {
						return record.Offset, nil
					}
					next = record.Offset + 1
				}
			}
		}
	}
}

func readEvent(poll entity.Poll, record *kgo.Record, visit func(entity.VoteSubmitted) error) (bool, error) {
	var value voteValue[*int]
	if err := json.Unmarshal(record.Value, &value); err != nil {
		return false, err
	}
	if value.PollID == uuid.Nil || value.Version < 1 {
		return false, errors.New("missing poll identity or version")
	}
	if value.PollID != poll.ID || value.Version != poll.Version {
		return false, nil
	}
	switch value.Type {
	case "CLOSE":
		if record.Key != nil || !value.ReceivedAt.IsZero() || value.Choices != nil {
			return false, errors.New("CLOSE marker contains ballot data")
		}
		return true, nil
	case "": // Ballots written by the intake endpoint have no type field.
		if len(record.Key) != 32 {
			return false, errors.New("invalid voter key length")
		}
		key := [32]byte(record.Key)
		if voterPartition(key, poll.Partitions) != record.Partition {
			return false, errors.New("voter key is in the wrong partition")
		}
		choices := make([]int, len(value.Choices))
		for i, id := range value.Choices {
			if id == nil {
				return false, nil // Invalid ballot; the previous valid choice still stands.
			}
			choices[i] = *id
		}
		return false, visit(entity.VoteSubmitted{
			PollID: value.PollID, Version: value.Version, ReceivedAt: value.ReceivedAt,
			VoterKey: key, Choices: choices,
		})
	default:
		return false, fmt.Errorf("unknown event type %q", value.Type)
	}
}
