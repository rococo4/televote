package kafka

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/rococo4/televote/internal/entity"
	"github.com/rococo4/televote/internal/service/counting"
)

type partitionReplay struct {
	id    int32
	next  int64
	visit func(entity.VoteSubmitted) error
	save  func(context.Context, int64) error
}

type countMember struct {
	poll         entity.Poll
	topicID      uuid.UUID
	callbacks    counting.GroupCallbacks
	owned        map[int32]bool
	done         map[int32]bool
	active       *partitionReplay
	nextProgress time.Time
}

func (p *Producer) ConsumePoll(ctx context.Context, poll entity.Poll, topicID uuid.UUID, cb counting.GroupCallbacks) error {
	if topicID == uuid.Nil || poll.Partitions < 1 {
		return errors.New("invalid topic identity or partition count for replay")
	}
	member := &countMember{poll: poll, topicID: topicID, callbacks: cb, owned: make(map[int32]bool)}
	reader, err := kgo.NewClient(
		kgo.SeedBrokers(p.brokers...), kgo.ConsumeTopics(poll.Topic),
		kgo.ConsumerGroup("televote-count-"+poll.ID.String()),
		kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(),
		// После CLOSE брокер может ещё ждать новых записей в старой партиции.
		// Короткий тайм-аут позволяет быстрее перейти к следующей.
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.ConsumeStartOffset(kgo.NewOffset().At(0)), kgo.ConsumeResetOffset(kgo.NoResetOffset()),
		kgo.AdjustFetchOffsetsFn(replayFromStart),
		kgo.OnPartitionsAssigned(member.assign), kgo.OnPartitionsRevoked(member.revoke), kgo.OnPartitionsLost(member.revoke),
	)
	if err != nil {
		return err
	}
	defer reader.CloseAllowingRebalance()

	for ctx.Err() == nil {
		// Даже участник без работы должен заметить, что другие реплики закончили
		// подсчёт. Тайм-аут позволяет проверить это, пока из Kafka нет данных.
		pollCtx, cancel := context.WithTimeout(ctx, time.Second)
		fetches := reader.PollRecords(pollCtx, 1024)
		cancel()
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(fetches) > 0 {
			// Пока обрабатываем эту пачку, Kafka-клиент откладывает смену
			// назначений до AllowRebalance. Гарантия действует и для ответа
			// с тайм-аутом, поэтому здесь можно работать с member без блокировки.
			if err := member.consume(ctx, reader, fetches); err != nil {
				return err
			}
			finished, err := member.selectPartition(ctx, reader)
			if err != nil || finished {
				return err
			}
		}
		reader.AllowRebalance()
	}
	return ctx.Err()
}

// Последний выбор каждого зрителя хранится в памяти читателя. Новый владелец
// перечитывает незавершённую партицию с начала, чтобы восстановить этот выбор.
// Партиции с готовым итогом остаются на паузе.
func replayFromStart(_ context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	for _, parts := range offsets {
		for id := range parts {
			parts[id] = kgo.NewOffset().At(0)
		}
	}
	return offsets, nil
}

func (m *countMember) assign(_ context.Context, reader *kgo.Client, assignment map[string][]int32) {
	reader.PauseFetchPartitions(assignment)
	for _, id := range assignment[m.poll.Topic] {
		m.owned[id] = true
	}
	m.nextProgress = time.Time{}
}

func (m *countMember) revoke(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	for _, id := range revoked[m.poll.Topic] {
		delete(m.owned, id)
		if m.active != nil && m.active.id == id {
			m.active = nil
		}
	}
}

func (m *countMember) selectPartition(ctx context.Context, reader *kgo.Client) (bool, error) {
	// После назначения партиций сразу сверяемся с PostgreSQL. Пока ждём
	// остальных участников, достаточно проверять готовые итоги раз в секунду.
	if m.nextProgress.IsZero() || (m.active == nil && time.Now().After(m.nextProgress)) {
		final, done, err := m.callbacks.Progress(ctx)
		if err != nil || final {
			return final, err
		}
		m.done = make(map[int32]bool, len(done))
		maps.Copy(m.done, done)
		m.nextProgress = time.Now().Add(time.Second)
	}
	if len(m.done) == m.poll.Partitions {
		return true, nil
	}
	if m.active == nil {
		for id := range int32(m.poll.Partitions) {
			if m.owned[id] && !m.done[id] {
				visit, save := m.callbacks.Start(id)
				m.active = &partitionReplay{id: id, visit: visit, save: save}
				reader.ResumeFetchPartitions(map[string][]int32{m.poll.Topic: {id}})
				break
			}
		}
	}
	return false, nil
}

func (m *countMember) consume(ctx context.Context, reader *kgo.Client, fetches kgo.Fetches) error {
	for _, e := range fetches.Errors() {
		if errors.Is(e.Err, context.DeadlineExceeded) && e.Topic == "" {
			return nil // Истёк тайм-аут чтения. Теперь можно проверить готовность итогов в PostgreSQL.
		}
		return fmt.Errorf("fetch %s[%d]: %w", e.Topic, e.Partition, e.Err)
	}
	for _, fetch := range fetches {
		for _, topic := range fetch.Topics {
			if topic.Topic != m.poll.Topic || topic.TopicID != m.topicID {
				return fmt.Errorf("topic identity changed during replay of %s", m.poll.Topic)
			}
			for _, part := range topic.Partitions {
				if len(part.Records) == 0 || m.done[part.Partition] {
					continue
				}
				if err := m.consumePartition(ctx, reader, part); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (m *countMember) consumePartition(ctx context.Context, reader *kgo.Client, part kgo.FetchPartition) error {
	active := m.active
	if active == nil || active.id != part.Partition {
		return fmt.Errorf("unexpected fetch for %s[%d]", m.poll.Topic, part.Partition)
	}
	if part.LogStartOffset > active.next {
		return fmt.Errorf("lost range [%d, %d) in %s[%d]", active.next, part.LogStartOffset, m.poll.Topic, active.id)
	}
	for _, record := range part.Records {
		if record.Offset != active.next {
			return fmt.Errorf("expected offset %d, got %d in %s[%d]", active.next, record.Offset, m.poll.Topic, active.id)
		}
		closed, err := readEvent(m.poll, record, active.visit)
		if err != nil {
			return fmt.Errorf("%s[%d] at offset %d: %w", m.poll.Topic, active.id, record.Offset, err)
		}
		if closed {
			if err := active.save(ctx, record.Offset); err != nil {
				return err
			}
			if err := reader.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf("commit CLOSE for %s[%d]: %w", m.poll.Topic, active.id, err)
			}
			m.done[active.id] = true
			reader.PauseFetchPartitions(map[string][]int32{m.poll.Topic: {active.id}})
			m.active = nil
			break
		}
		active.next++
	}
	return nil
}
