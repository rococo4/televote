//go:build integration

package integration_test

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rococo4/televote/internal/adapter/kafka"
	"github.com/rococo4/televote/internal/app"
	"github.com/rococo4/televote/internal/entity"
	"github.com/rococo4/televote/internal/repository/resultsrepo"
	"github.com/rococo4/televote/internal/service/voting"
)

func TestCountVotesAfterClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	f := newVoteFixture(t, ctx, 5*time.Second)
	p := f.poll
	if err := f.handler.CountClosedPoll(ctx, p.ID); !errors.Is(err, voting.ErrStillOpen) {
		t.Fatalf("count while open: got %v, want ErrStillOpen", err)
	}

	// Separate voters into known partitions so both the nonempty and empty
	// partitions have independently predictable closing offsets.
	x, y := tokenForPartition(p, 0), tokenForPartition(p, 1)
	path := f.server.URL + "/api/polls/" + p.ID.String() + "/votes"
	for _, vote := range []struct {
		token  string
		choice int
	}{{x, 1}, {x, 2}, {y, 2}} {
		postVote(t, ctx, f.server.Client(), path, vote.token, vote.choice)
	}
	time.Sleep(time.Until(p.ClosesAt))

	// Reproduce a stopped worker's durable state: partition 2 already has
	// its closing record and a committed zero result. No tests write counts
	// directly to SQL, so this also checks that partial results stay private.
	const finished int32 = 2
	store := resultsrepo.New(f.pool)
	topicID, bound, err := store.BoundTopic(ctx, p)
	if err != nil || !bound {
		t.Fatalf("load topic manifest: bound=%v, err=%v", bound, err)
	}
	// Partition 0 got its marker before the interruption, but has no SQL
	// result. Recovery must honor this first marker even if it sends another.
	if err := f.writer.MarkClosed(ctx, p, 0); err != nil {
		t.Fatal(err)
	}
	if err := f.writer.MarkClosed(ctx, p, finished); err != nil {
		t.Fatal(err)
	}
	end, err := f.writer.ReadPartition(ctx, p, topicID, finished, func(entity.VoteSubmitted) error {
		return errors.New("unexpected vote in the empty partition")
	})
	if err != nil || end != 0 {
		t.Fatalf("empty partition: close offset=%d, error=%v", end, err)
	}
	partial := entity.PartitionResult{Partition: finished, EndOffset: end, Counts: map[int]int64{1: 0, 2: 0}}
	if err := store.SavePartition(ctx, p, partial); err != nil {
		t.Fatal(err)
	}
	if err := store.Finalize(ctx, p); err == nil {
		t.Fatal("published a partial poll result")
	}
	var finals int
	if err := f.pool.QueryRow(ctx, "SELECT count(*) FROM poll_results WHERE poll_id = $1", p.ID).Scan(&finals); err != nil || finals != 0 {
		t.Fatalf("final rows before all partitions completed: %d, error=%v", finals, err)
	}

	if err := f.handler.CountClosedPoll(ctx, p.ID); err != nil {
		t.Fatal(err)
	}
	checkCountedPoll(t, ctx, f)

	// A fresh handler and Kafka client discover the persisted final in SQL.
	f.server.Close()
	producer, err := kafka.New(f.brokers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	restarted, err := app.NewHandler(ctx, f.pool, producer)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.CountClosedPoll(ctx, p.ID); err != nil {
		t.Fatalf("restart after final: %v", err)
	}
	checkCountedPoll(t, ctx, f)
	t.Log("3 HTTP 202s; 4 persisted partition results (2 empty); final 2 ballots, A=0 B=2 before and after restart")
}

func tokenForPartition(p entity.Poll, partition int) string {
	for {
		token := uuid.New()
		key := entity.VoterKey(p.ID, token)
		if binary.BigEndian.Uint64(key[:8])%uint64(p.Partitions) == uint64(partition) {
			return hex.EncodeToString(token[:])
		}
	}
}

func checkCountedPoll(t *testing.T, ctx context.Context, f *voteFixture) {
	t.Helper()
	var ballots int64
	var counts map[int]int64
	if err := f.pool.QueryRow(ctx, `
		SELECT ballots, counts FROM poll_results WHERE poll_id = $1 AND version = $2 AND algorithm = 1`,
		f.poll.ID, f.poll.Version).Scan(&ballots, &counts); err != nil {
		t.Fatal(err)
	}
	if ballots != 2 || !maps.Equal(counts, map[int]int64{1: 0, 2: 2}) {
		t.Fatalf("final: ballots=%d counts=%v, want 2 ballots, A=0 B=2", ballots, counts)
	}

	rows, err := f.pool.Query(ctx, `
		SELECT partition_id, start_offset, end_offset, ballots, counts FROM poll_partition_results
		WHERE poll_id = $1 AND version = $2 AND algorithm = 1 ORDER BY partition_id`, f.poll.ID, f.poll.Version)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	wantEnds := []int64{2, 1, 0, 0}
	wantBallots := []int64{1, 1, 0, 0}
	for i := range partitions {
		var part int
		var start, end, n int64
		var choices map[int]int64
		if !rows.Next() {
			t.Fatalf("missing partition %d: %v", i, rows.Err())
		}
		if err := rows.Scan(&part, &start, &end, &n, &choices); err != nil {
			t.Fatal(err)
		}
		if part != i || start != 0 || end != wantEnds[i] || n != wantBallots[i] || !maps.Equal(choices, map[int]int64{1: 0, 2: wantBallots[i]}) {
			t.Fatalf("unexpected partition result: id=%d range=[%d,%d) ballots=%d counts=%v", part, start, end, n, choices)
		}
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatalf("unexpected extra row or query error: %v", rows.Err())
	}
}
