//go:build integration

package resultsrepo_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rococo4/televote/internal/entity"
	"github.com/rococo4/televote/internal/repository/resultsrepo"
)

// A result is publishable only when every partition, including an empty one,
// agrees with its durable checkpoint. A failed/retried write cannot replace it.
func TestPartitionCheckpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://televote:televote@127.0.0.1:55432/televote?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	p := entity.Poll{ID: uuid.New(), Version: 3, Partitions: 2, Choices: map[int]struct{}{1: {}, 2: {}}}
	p.Topic = "checkpoint-" + p.ID.String()
	_, err = pool.Exec(ctx, `
		INSERT INTO polls (id, version, question, opens_at, closes_at, min_choices, max_choices, topic, partitions)
		VALUES ($1,$2,'A/B',now(),now() + interval '1 hour',1,1,$3,$4)`, p.ID, p.Version, p.Topic, p.Partitions)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := pool.Exec(ctx, "DELETE FROM polls WHERE id = $1", p.ID); err != nil {
			t.Errorf("clean up test poll: %v", err)
		}
	}()

	repo := resultsrepo.New(pool)
	topicID := uuid.New()
	if err := repo.Bind(ctx, p, topicID); err != nil {
		t.Fatal(err)
	}
	if err := repo.Bind(ctx, p, uuid.New()); err == nil {
		t.Fatal("replaced a bound topic UUID")
	}
	first := entity.PartitionResult{Partition: 0, EndOffset: 3, Ballots: 2, Counts: map[int]int64{1: 0, 2: 2}}
	for range 2 {
		if err := repo.SavePartition(ctx, p, first); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.Finalize(ctx, p); err == nil {
		t.Fatal("published final before the empty partition finished")
	}
	final, done, err := repo.Progress(ctx, p)
	if err != nil || final || len(done) != 1 || !done[0] {
		t.Fatalf("partial progress: final=%v done=%v err=%v", final, done, err)
	}
	empty := entity.PartitionResult{Partition: 1, Counts: map[int]int64{1: 0, 2: 0}}
	if err := repo.SavePartition(ctx, p, empty); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := repo.Finalize(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	first.Counts = map[int]int64{1: 1, 2: 1}
	if err := repo.SavePartition(ctx, p, first); err == nil {
		t.Fatal("replay overwrote an existing partition with conflicting counts")
	}
	var ballots, a, b int64
	if err := pool.QueryRow(ctx, `
		SELECT ballots, (counts->>'1')::bigint, (counts->>'2')::bigint
		FROM poll_results WHERE poll_id = $1 AND version = $2 AND algorithm = 1`,
		p.ID, p.Version).Scan(&ballots, &a, &b); err != nil {
		t.Fatal(err)
	}
	if ballots != 2 || a != 0 || b != 2 {
		t.Fatalf("final=%d ballots, A=%d B=%d", ballots, a, b)
	}
}
