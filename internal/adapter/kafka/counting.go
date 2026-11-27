package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/rococo4/televote/internal/entity"
)

// TopicID проверяет, что состав партиций совпадает с настройками опроса,
// и возвращает UUID топика. При пересоздании топика под тем же именем UUID изменится.
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
	value, err := json.Marshal(eventHeader{Type: "CLOSE", PollID: poll.ID})
	if err != nil {
		return err
	}
	record := &kgo.Record{Topic: poll.Topic, Partition: partition, Value: value}
	return p.client.ProduceSync(ctx, record).FirstErr()
}

func readEvent(poll entity.Poll, record *kgo.Record, visit func(entity.VoteSubmitted) error) (bool, error) {
	var value struct {
		eventHeader
		ReceivedAt time.Time `json:"received_at"`
		Choices    []*int    `json:"choices"` // nil отличает JSON null от допустимого ID 0.
	}
	if err := json.Unmarshal(record.Value, &value); err != nil {
		return false, err
	}
	if value.PollID == uuid.Nil {
		return false, errors.New("missing poll ID")
	}
	if value.PollID != poll.ID {
		return false, nil
	}
	switch value.Type {
	case "CLOSE":
		if record.Key != nil || !value.ReceivedAt.IsZero() || value.Choices != nil {
			return false, errors.New("CLOSE marker contains ballot data")
		}
		return true, nil
	case "": // При записи бюллетеня API опускает поле type.
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
				return false, nil // Бюллетень с null пропускаем целиком, сохраняя предыдущий выбор зрителя.
			}
			choices[i] = *id
		}
		return false, visit(entity.VoteSubmitted{
			PollID: value.PollID, ReceivedAt: value.ReceivedAt,
			VoterKey: key, Choices: choices,
		})
	default:
		return false, fmt.Errorf("unknown event type %q", value.Type)
	}
}
