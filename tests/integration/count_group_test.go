//go:build integration

package integration_test

import (
	"context"
	"errors"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/rococo4/televote/internal/adapter/kafka"
	"github.com/rococo4/televote/internal/entity"
	"github.com/rococo4/televote/internal/repository/resultsrepo"
	"github.com/rococo4/televote/internal/service/counting"
)

func TestCountConsumerGroupFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	f := newVoteFixture(t, ctx, 4*time.Second)
	p := f.poll
	path := f.server.URL + "/api/polls/" + p.ID.String() + "/votes"
	for part := range p.Partitions {
		// В каждой партиции первый зритель голосует за A, второй меняет A на B.
		// После передачи партиции другому читателю ожидаем по одному голосу за A и B.
		changing := tokenForPartition(p, part)
		postVote(t, ctx, f.server.Client(), path, tokenForPartition(p, part), 1)
		postVote(t, ctx, f.server.Client(), path, changing, 1)
		postVote(t, ctx, f.server.Client(), path, changing, 2)
	}
	time.Sleep(time.Until(p.ClosesAt))

	store := resultsrepo.New(f.pool)
	failing := &interruptedSave{Repository: store, partition: make(chan int32, 1)}
	aCtx, stopA := context.WithCancel(ctx)
	defer stopA()
	var counters sync.WaitGroup
	t.Cleanup(func() { cancel(); counters.Wait() })
	run := func(ctx context.Context, results counting.Results) <-chan error {
		done := make(chan error, 1)
		service := counting.New(groupTestJournal{f.writer}, results)
		counters.Go(func() { done <- service.Count(ctx, p) })
		return done
	}
	aDone := run(aCtx, failing)
	bDone := run(ctx, store)

	// Отправим CLOSE, когда Kafka назначит партиции обоим читателям.
	// Иначе первый может успеть посчитать весь тестовый опрос до подключения второго.
	groupID := "televote-count-" + p.ID.String()
	waitForSharedAssignment(t, ctx, f, groupID)
	for part := range int32(p.Partitions) {
		if err := f.writer.MarkClosed(ctx, p, part); err != nil {
			t.Fatal(err)
		}
	}
	var interrupted int32
	select {
	case interrupted = <-failing.partition:
	case err := <-aDone:
		t.Fatalf("first counter stopped before reaching a partition boundary: %v", err)
	case <-ctx.Done():
		t.Fatal("first counter never read CLOSE")
	}
	if offset, ok := committedOffsets(t, ctx, f, groupID)[interrupted]; ok && offset != -1 {
		t.Fatalf("partition %d committed offset %d before saving its result", interrupted, offset)
	}
	stopA()
	if err := <-aDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted counter returned %v", err)
	}
	if err := <-bDone; err != nil {
		t.Fatalf("surviving counter: %v", err)
	}
	offsets := committedOffsets(t, ctx, f, groupID)
	for part := range int32(partitions) {
		// Три голоса занимают позиции 0–2, CLOSE занимает позицию 3. Читатель должен подтвердить позицию 4.
		if offsets[part] != 4 {
			t.Fatalf("partition %d committed offset %d, want 4", part, offsets[part])
		}
	}

	var ballots, recovered, completed int
	var counts map[int]int64
	err := f.pool.QueryRow(ctx, `SELECT r.ballots, r.counts,
		(SELECT count(*) FROM poll_partition_results WHERE poll_id=r.poll_id),
		(SELECT ballots FROM poll_partition_results WHERE poll_id=r.poll_id AND partition_id=$2)
		FROM poll_results r WHERE poll_id=$1`, p.ID, interrupted).Scan(&ballots, &counts, &completed, &recovered)
	if err != nil {
		t.Fatal(err)
	}
	if completed != partitions || recovered != 2 || ballots != 2*partitions ||
		!maps.Equal(counts, map[int]int64{1: partitions, 2: partitions}) {
		t.Fatalf("unexpected result after takeover: %d partitions, %d recovered ballots, final %d / %v", completed, recovered, ballots, counts)
	}
}

// Тест сам отправит CLOSE, когда обе реплики получат партиции.
type groupTestJournal struct{ *kafka.Producer }

func (groupTestJournal) MarkClosed(context.Context, entity.Poll, int32) error { return nil }

type interruptedSave struct {
	*resultsrepo.Repository
	partition chan int32
}

func (s *interruptedSave) SavePartition(ctx context.Context, _ entity.Poll, result entity.PartitionResult) error {
	s.partition <- result.Partition
	<-ctx.Done()
	return ctx.Err()
}

func committedOffsets(t *testing.T, ctx context.Context, f *voteFixture, groupID string) map[int32]int64 {
	t.Helper()
	group := kmsg.NewOffsetFetchRequestGroup()
	group.Group = groupID
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Groups = []kmsg.OffsetFetchRequestGroup{group}
	resp, err := req.RequestWith(ctx, f.reader)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Groups) != 1 || resp.Groups[0].Group != groupID || resp.Groups[0].ErrorCode != 0 {
		t.Fatalf("unexpected OffsetFetch response: %+v", resp.Groups)
	}
	offsets := make(map[int32]int64)
	for _, topic := range resp.Groups[0].Topics {
		for _, part := range topic.Partitions {
			if err := kerr.ErrorForCode(part.ErrorCode); err != nil {
				t.Fatalf("fetch offset for partition %d: %v", part.Partition, err)
			}
			offsets[part.Partition] = part.Offset
		}
	}
	return offsets
}

func waitForSharedAssignment(t *testing.T, ctx context.Context, f *voteFixture, groupID string) {
	t.Helper()
	req := kmsg.NewPtrDescribeGroupsRequest()
	req.Groups = []string{groupID}
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		resp, err := req.RequestWith(ctx, f.reader)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Groups) != 1 {
			t.Fatalf("unexpected DescribeGroups response: %+v", resp.Groups)
		}
		group := resp.Groups[0]
		err = kerr.ErrorForCode(group.ErrorCode)
		if err != nil && !errors.Is(err, kerr.GroupIDNotFound) && !kerr.IsRetriable(err) {
			t.Fatal(err)
		}
		if err == nil && group.State == "Stable" && len(group.Members) == 2 {
			parts := make(map[int32]bool)
			owners := 0
			for _, member := range group.Members {
				var assigned kmsg.ConsumerMemberAssignment
				if err := assigned.ReadFrom(member.MemberAssignment); err != nil {
					t.Fatal(err)
				}
				if len(assigned.Topics) == 0 {
					continue
				}
				if len(assigned.Topics) != 1 || assigned.Topics[0].Topic != f.poll.Topic {
					t.Fatalf("member %s has unexpected assignment %+v", member.MemberID, assigned.Topics)
				}
				if len(assigned.Topics[0].Partitions) > 0 {
					owners++
				}
				for _, id := range assigned.Topics[0].Partitions {
					if id < 0 || int(id) >= f.poll.Partitions || parts[id] {
						t.Fatalf("unexpected partition assignment %d", id)
					}
					parts[id] = true
				}
			}
			// При поэтапной передаче Kafka сначала отзывает партицию у прежнего владельца.
			// Назначение новому участнику может появиться в следующем поколении группы.
			if owners == 2 && len(parts) == f.poll.Partitions {
				return
			}
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatalf("waiting for two consumers to split %d partitions: %v", f.poll.Partitions, ctx.Err())
		}
	}
}
