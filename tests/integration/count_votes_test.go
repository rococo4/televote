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
	"github.com/rococo4/televote/internal/repository/closurerepo"
	"github.com/rococo4/televote/internal/repository/resultsrepo"
	"github.com/rococo4/televote/internal/service/voting"
)

func TestCountVotesAfterClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	f := newVoteFixture(t, ctx, 5*time.Second)
	p := f.poll
	if err := f.application.CountClosedPoll(ctx, p.ID); !errors.Is(err, voting.ErrStillOpen) {
		t.Fatalf("count while open: got %v, want ErrStillOpen", err)
	}

	// Подбираем токены для партиций 0 и 1. Остальные останутся пустыми,
	// поэтому позицию CLOSE в каждой партиции можно предсказать заранее.
	x, y := tokenForPartition(p, 0), tokenForPartition(p, 1)
	path := f.server.URL + "/api/polls/" + p.ID.String() + "/votes"
	for _, vote := range []struct {
		token  string
		choice int
	}{{x, 1}, {x, 2}, {y, 2}} {
		postVote(t, ctx, f.server.Client(), path, vote.token, vote.choice)
	}
	// А завершит отправки первым. Последний писатель Б примет решение,
	// успеет отправить только часть маркеров и остановится.
	b := uuid.New()
	closures := closurerepo.New(f.pool, time.Hour)
	if joined, _, err := closures.Join(ctx, p, b); err != nil || !joined {
		t.Fatalf("register writer B: joined=%v, err=%v", joined, err)
	}
	stopA := startPollWorker(t, ctx, f.application)
	waitPG(ctx, t, f, `SELECT frozen AND decided_at IS NULL FROM poll_closures WHERE poll_id=$1`)
	if err := closures.Drain(ctx, p, b); err != nil {
		t.Fatal(err)
	}
	if ready, _, err := closures.Decide(ctx, p); err != nil || !ready {
		t.Fatalf("last writer's decision: ready=%v, err=%v", ready, err)
	}

	// Воспроизводим остановку между партициями. Для пустой партиции 2 уже сохранён
	// нулевой итог, но результат всего опроса публиковать ещё рано.
	const finished int32 = 2
	store := resultsrepo.New(f.pool)
	// В партиции 0 после голосов уже стоит CLOSE, а её итог ещё не сохранён.
	// При повторной попытке границу подсчёта по-прежнему задаёт первый CLOSE.
	if err := f.writer.MarkClosed(ctx, p, 0); err != nil {
		t.Fatal(err)
	}
	if err := f.writer.MarkClosed(ctx, p, finished); err != nil {
		t.Fatal(err)
	}
	// В пустой партиции 2 маркер занял позицию 0, поэтому следующая позиция равна 1.
	assertPartitionEnds(t, ctx, f, []int64{3, 1, 1, 0})
	partial := entity.PartitionResult{Partition: finished, EndOffset: 0, Counts: map[int]int64{1: 0, 2: 0}}
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

	// Под А уже ждал общего срока в момент остановки Б. Он подхватит
	// оставшиеся партиции по сохранённому решению.
	waitForFinal(t, ctx, f)
	checkCountedPoll(t, ctx, f)

	// После перезапуска обработчик должен найти сохранённый итог в PostgreSQL.
	stopA()
	f.server.Close()
	producer, err := kafka.New(f.brokers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	restarted, err := app.New(ctx, f.pool, producer, testCloseGrace, app.AdminConfig{Partitions: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.CountClosedPoll(ctx, p.ID); err != nil {
		t.Fatalf("restart after final: %v", err)
	}
	checkCountedPoll(t, ctx, f)
	t.Log("3 HTTP 202s; 4 persisted partition results (2 empty); final 2 ballots, A=0 B=2 before and after restart")

	t.Run("result predating writer registration", func(t *testing.T) {
		// У опросов, завершённых до миграции 003, уже есть итог, а записи в poll_closures ещё нет.
		if _, err := f.pool.Exec(ctx, "DELETE FROM poll_closures WHERE poll_id=$1", p.ID); err != nil {
			t.Fatal(err)
		}
		if err := restarted.CountClosedPoll(ctx, p.ID); err != nil {
			t.Fatal(err)
		}
		var exists bool
		if err := f.pool.QueryRow(ctx, "SELECT EXISTS(SELECT FROM poll_closures WHERE poll_id=$1)", p.ID).Scan(&exists); err != nil || exists {
			t.Fatalf("retroactive close decision: exists=%v err=%v", exists, err)
		}
		checkCountedPoll(t, ctx, f)
	})
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
		SELECT ballots, counts FROM poll_results WHERE poll_id = $1`,
		f.poll.ID).Scan(&ballots, &counts); err != nil {
		t.Fatal(err)
	}
	if ballots != 2 || !maps.Equal(counts, map[int]int64{1: 0, 2: 2}) {
		t.Fatalf("final: ballots=%d counts=%v, want 2 ballots, A=0 B=2", ballots, counts)
	}

	rows, err := f.pool.Query(ctx, `
		SELECT partition_id, end_offset, ballots, counts FROM poll_partition_results
		WHERE poll_id = $1 ORDER BY partition_id`, f.poll.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	wantEnds := []int64{2, 1, 0, 0}
	wantBallots := []int64{1, 1, 0, 0}
	for i := range partitions {
		var part int
		var end, n int64
		var choices map[int]int64
		if !rows.Next() {
			t.Fatalf("missing partition %d: %v", i, rows.Err())
		}
		if err := rows.Scan(&part, &end, &n, &choices); err != nil {
			t.Fatal(err)
		}
		if part != i || end != wantEnds[i] || n != wantBallots[i] || !maps.Equal(choices, map[int]int64{1: 0, 2: wantBallots[i]}) {
			t.Fatalf("unexpected partition result: id=%d end_offset=%d ballots=%d counts=%v", part, end, n, choices)
		}
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatalf("unexpected extra row or query error: %v", rows.Err())
	}
}
