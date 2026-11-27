//go:build integration

package kafka

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rococo4/televote/internal/entity"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Сбой между созданием топика и публикацией опроса оставляет топик в Kafka.
// При повторе проверяем число партиций и политику delete. Уплотнение журнала
// по ключу зрителя оставило бы пропуски в последовательности записей.
func TestEnsureExistingTopic(t *testing.T) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "127.0.0.1:59092"
	}
	producer, err := New(strings.Split(brokers, ","))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	for _, policy := range []string{"delete", "compact"} {
		t.Run(policy, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			poll := entity.Poll{Topic: "televote-provision-" + uuid.NewString(), Partitions: 2}
			req := kmsg.NewPtrCreateTopicsRequest()
			req.Topics = []kmsg.CreateTopicsRequestTopic{{
				Topic: poll.Topic, NumPartitions: 2, ReplicationFactor: -1,
				Configs: []kmsg.CreateTopicsRequestTopicConfig{{Name: "cleanup.policy", Value: &policy}},
			}}
			res, err := req.RequestWith(ctx, producer.client)
			if err != nil || len(res.Topics) != 1 || res.Topics[0].ErrorCode != 0 {
				t.Fatalf("create journal: %+v %v", res, err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				del := kmsg.NewPtrDeleteTopicsRequest()
				del.TopicNames = []string{poll.Topic}
				del.Topics = []kmsg.DeleteTopicsRequestTopic{{Topic: &poll.Topic}}
				res, err := del.RequestWith(ctx, producer.client)
				if err != nil || len(res.Topics) != 1 || res.Topics[0].ErrorCode != 0 {
					t.Errorf("delete journal: %+v %v", res, err)
				}
			})
			id, err := producer.EnsureTopic(ctx, poll)
			if policy == "compact" {
				if err == nil || !strings.Contains(err.Error(), "cleanup.policy") {
					t.Fatalf("accepted compacted journal or failed for unrelated reason: %v", err)
				}
				return
			}
			if err != nil || id == uuid.Nil {
				t.Fatalf("recover existing journal: %s %v", id, err)
			}
			poll.Partitions++
			if _, err := producer.EnsureTopic(ctx, poll); err == nil {
				t.Fatal("accepted a different number of partitions")
			}
		})
	}
}
