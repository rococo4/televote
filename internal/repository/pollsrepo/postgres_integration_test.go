//go:build integration

package pollsrepo_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rococo4/televote/internal/repository/pollsrepo"
)

func TestLoadPublished(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://televote:televote@127.0.0.1:55432/televote?sslmode=disable"
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	// Apply the production migration in a schema owned by this test.
	schema := pgx.Identifier{"televote_test_" + uuid.NewString()}.Sanitize()
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if _, err := pool.Exec(cleanup, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("clean up test schema: %v", err)
		}
	}()
	migration, err := os.ReadFile("../../migrations/postgres/001_create_polls.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}

	id := uuid.New()
	open := time.Now().UTC().Truncate(time.Second)
	closeAt := open.Add(time.Minute)
	if _, err := pool.Exec(ctx, `
		INSERT INTO polls (id, version, question, opens_at, closes_at, min_choices, max_choices, topic, partitions, published)
		VALUES ($1, 7, 'A/B?', $2, $3, 1, 1, 'open-poll', 3, true),
		       ($4, 1, 'Draft', $2, $3, 1, 1, 'draft-poll', 2, false)`, id, open, closeAt, uuid.New()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO poll_choices (poll_id, id, label) VALUES ($1, 1, 'A'), ($1, 2, 'B')", id); err != nil {
		t.Fatal(err)
	}
	polls, err := pollsrepo.New(pool).LoadPublished(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(polls) != 1 {
		t.Fatalf("loaded %d polls, want exactly one published poll", len(polls))
	}
	poll := polls[0]
	if poll.ID != id || poll.Version != 7 || poll.Topic != "open-poll" || poll.Partitions != 3 ||
		!poll.OpensAt.Equal(open) || !poll.ClosesAt.Equal(closeAt) || poll.MinChoices != 1 || poll.MaxChoices != 1 {
		t.Fatalf("unexpected published configuration: %+v", poll)
	}
	_, hasA := poll.Choices[1]
	_, hasB := poll.Choices[2]
	if !hasA || !hasB || len(poll.Choices) != 2 {
		t.Fatalf("loaded choices: %v, want 1 and 2", poll.Choices)
	}
}
