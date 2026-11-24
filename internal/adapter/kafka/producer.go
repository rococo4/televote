package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/rococo4/televote/internal/entity"
)

type Producer struct {
	client     *kgo.Client
	brokers    []string
	deliveries sync.Map // poll UUID -> *sync.WaitGroup for Kafka delivery callbacks
}

func New(brokers []string) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("no Kafka brokers configured")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.MaxBufferedRecords(16_384),
		kgo.MaxBufferedBytes(32<<20),
		kgo.ProducerLinger(2*time.Millisecond),
		kgo.RecordDeliveryTimeout(5*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	return &Producer{client: client, brokers: slices.Clone(brokers)}, nil
}

func (p *Producer) Ping(ctx context.Context) error {
	return p.client.Ping(ctx)
}

func (p *Producer) Flush(ctx context.Context) error {
	return p.client.Flush(ctx)
}

func (p *Producer) Close() {
	p.client.Close()
}

type eventHeader struct {
	Type    string    `json:"type,omitempty"`
	PollID  uuid.UUID `json:"poll_id"`
	Version int64     `json:"version"`
}

// The reader uses *int choices to distinguish JSON null from choice ID 0.
type voteValue[T int | *int] struct {
	eventHeader
	ReceivedAt time.Time `json:"received_at"`
	Choices    []T       `json:"choices"`
}

func (p *Producer) Publish(ctx context.Context, poll entity.Poll, vote entity.VoteSubmitted) error {
	if poll.Partitions < 1 {
		return fmt.Errorf("invalid partition count %d for poll %s", poll.Partitions, poll.ID)
	}
	value, err := json.Marshal(voteValue[int]{
		eventHeader: eventHeader{PollID: vote.PollID, Version: vote.Version},
		ReceivedAt:  vote.ReceivedAt,
		Choices:     vote.Choices,
	})
	if err != nil {
		return fmt.Errorf("marshal vote: %w", err)
	}

	partition := voterPartition(vote.VoterKey, poll.Partitions)
	record := &kgo.Record{
		Topic:     poll.Topic,
		Partition: partition,
		Key:       vote.VoterKey[:],
		Value:     value,
	}
	result := make(chan error, 1)
	active, ok := p.deliveries.Load(poll.ID)
	if !ok {
		active, _ = p.deliveries.LoadOrStore(poll.ID, new(sync.WaitGroup))
	}
	pending := active.(*sync.WaitGroup)
	pending.Add(1)
	p.client.TryProduce(ctx, record, func(_ *kgo.Record, err error) {
		result <- err
		pending.Done()
	})
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		// A late ACK is still possible. Flush waits for outstanding sends on shutdown.
		return ctx.Err()
	}
}

func voterPartition(key [32]byte, partitions int) int32 {
	return int32(binary.BigEndian.Uint64(key[:8]) % uint64(partitions))
}

// WaitVotes is called after admission for this poll is sealed and its
// handlers have finished. An HTTP timeout may precede the delivery callback.
func (p *Producer) WaitVotes(ctx context.Context, pollID uuid.UUID) error {
	active, ok := p.deliveries.Load(pollID)
	if !ok {
		return nil
	}
	done := make(chan struct{})
	go func() {
		active.(*sync.WaitGroup).Wait()
		close(done)
	}()
	select {
	case <-done:
		p.deliveries.Delete(pollID)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
