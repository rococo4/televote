package kafka

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/rococo4/televote/internal/entity"
)

// EnsureTopic создаёт для опроса отдельный топик с политикой очистки delete.
// Если предыдущая попытка создала топик, но не опубликовала опрос в базе,
// проверяем настройки существующего топика перед повторным использованием.
func (p *Producer) EnsureTopic(ctx context.Context, poll entity.Poll) (uuid.UUID, error) {
	if poll.Partitions < 1 || poll.Partitions > math.MaxInt32 {
		return uuid.Nil, fmt.Errorf("invalid number of Kafka partitions: %d", poll.Partitions)
	}
	create := kmsg.NewPtrCreateTopicsRequest()
	cleanup := "delete"
	create.Topics = []kmsg.CreateTopicsRequestTopic{{
		Topic: poll.Topic, NumPartitions: int32(poll.Partitions), ReplicationFactor: -1,
		Configs: []kmsg.CreateTopicsRequestTopicConfig{{Name: "cleanup.policy", Value: &cleanup}},
	}}
	res, err := create.RequestWith(ctx, p.client)
	if err != nil {
		return uuid.Nil, fmt.Errorf("create topic %s: %w", poll.Topic, err)
	}
	if len(res.Topics) != 1 || res.Topics[0].Topic != poll.Topic {
		return uuid.Nil, fmt.Errorf("unexpected CreateTopics response for %s", poll.Topic)
	}
	createErr := kerr.ErrorForCode(res.Topics[0].ErrorCode)
	if createErr != nil && !errors.Is(createErr, kerr.TopicAlreadyExists) {
		return uuid.Nil, fmt.Errorf("create topic %s: %w", poll.Topic, createErr)
	}
	// После создания топика сведения о его партициях могут дойти до брокера с задержкой.
	for attempt := 0; ; attempt++ {
		var id uuid.UUID
		id, err = p.TopicID(ctx, poll)
		if err == nil && createErr != nil {
			err = p.checkCleanupPolicy(ctx, poll.Topic)
		}
		if err == nil {
			return id, nil
		}
		if !kerr.IsRetriable(err) || attempt == 5 {
			return uuid.Nil, err
		}
		select {
		case <-ctx.Done():
			return uuid.Nil, ctx.Err()
		case <-time.After(50 * time.Millisecond << attempt):
		}
	}
}

func (p *Producer) checkCleanupPolicy(ctx context.Context, topic string) error {
	req := kmsg.NewPtrDescribeConfigsRequest()
	req.Resources = []kmsg.DescribeConfigsRequestResource{{
		ResourceType: kmsg.ConfigResourceTypeTopic, ResourceName: topic, ConfigNames: []string{"cleanup.policy"},
	}}
	res, err := req.RequestWith(ctx, p.client)
	if err != nil {
		return err
	}
	if len(res.Resources) != 1 || res.Resources[0].ResourceName != topic {
		return fmt.Errorf("unexpected DescribeConfigs response for %s", topic)
	}
	resource := res.Resources[0]
	if err := kerr.ErrorForCode(resource.ErrorCode); err != nil {
		return fmt.Errorf("describe topic %s: %w", topic, err)
	}
	for _, cfg := range resource.Configs {
		if cfg.Name == "cleanup.policy" && cfg.Value != nil && *cfg.Value == "delete" {
			return nil
		}
	}
	return fmt.Errorf("topic %s requires cleanup.policy=delete", topic)
}
