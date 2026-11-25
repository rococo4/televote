//go:build integration

package pollsrepo_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rococo4/televote/internal/entity"
	"github.com/rococo4/televote/internal/repository/pollsrepo"
	"github.com/rococo4/televote/internal/repository/resultsrepo"
)

func pollTestDB(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
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
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if _, err := pool.Exec(cleanup, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("clean up test schema: %v", err)
		}
	})
	for _, name := range []string{"001_create_polls.sql", "002_count_results.sql"} {
		migration, err := os.ReadFile("../../migrations/postgres/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(migration)); err != nil {
			t.Fatal(err)
		}
	}
	return ctx, pool
}

func TestLoadPublished(t *testing.T) {
	ctx, pool := pollTestDB(t)
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

func TestDraftPublicationAndRecovery(t *testing.T) {
	ctx, db := pollTestDB(t)
	repo := pollsrepo.New(db)
	var now time.Time
	if err := db.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		t.Fatal(err)
	}
	spec := entity.CreatePoll{
		ID: uuid.New(), Question: "Choose?", Choices: []string{"A", "B"},
		OpensAt: now.Add(-time.Hour), ClosesAt: now.Add(-time.Second), MinChoices: 1, MaxChoices: 1,
	}
	if _, _, err := repo.Draft(ctx, spec, 2); !errors.Is(err, entity.ErrInvalidPoll) {
		t.Fatalf("first creation after voting window: %v", err)
	}
	spec.ClosesAt = now.Add(time.Hour)
	poll, published, err := repo.Draft(ctx, spec, 2)
	if err != nil || published {
		t.Fatalf("draft: published=%t err=%v", published, err)
	}
	if _, err := repo.Results(ctx, poll.ID); !errors.Is(err, entity.ErrPollNotFound) {
		t.Fatalf("draft report: %v", err)
	}
	if err := repo.Publish(ctx, poll, uuid.Nil); err == nil {
		t.Fatal("published with unbound journal")
	}
	if _, err := repo.FindPublished(ctx, poll.ID); !errors.Is(err, entity.ErrPollNotFound) {
		t.Fatalf("publication after failed bind: %v", err)
	}
	retry, published, err := repo.Draft(ctx, spec, 7)
	if err != nil || published || retry.Partitions != 2 {
		t.Fatalf("draft retry with new configuration: published=%t poll=%+v err=%v", published, retry, err)
	}
	changed := spec
	changed.Choices = []string{"B", "A"}
	if _, _, err := repo.Draft(ctx, changed, 2); !errors.Is(err, entity.ErrPollConflict) {
		t.Fatalf("conflicting definition: %v", err)
	}
	topicID := uuid.New()
	if err := repo.Publish(ctx, poll, topicID); err != nil {
		t.Fatal(err)
	}
	if err := repo.Publish(ctx, poll, uuid.New()); err == nil {
		t.Fatal("rebound published poll to a new topic")
	}
	report, err := repo.Results(ctx, poll.ID)
	if err != nil || report.Status != "open" || report.Ballots != nil || len(report.Choices) != 2 || report.Choices[0].Votes != nil {
		t.Fatalf("open report: %+v %v", report, err)
	}
	// Put the same definition in the past without a timing-dependent test sleep.
	spec.ClosesAt = now.Add(-time.Second)
	if _, err := db.Exec(ctx, "UPDATE polls SET closes_at=$2 WHERE id=$1", poll.ID, spec.ClosesAt); err != nil {
		t.Fatal(err)
	}
	if _, pub, err := repo.Draft(ctx, spec, 5); err != nil || !pub {
		t.Fatalf("retry after deadline: %t %v", pub, err)
	}
	unfinished, err := repo.LoadUnfinished(ctx)
	if err != nil || len(unfinished) != 1 || unfinished[0].ID != poll.ID {
		t.Fatalf("discover unfinished closed poll: %v %v", unfinished, err)
	}
	report, err = repo.Results(ctx, poll.ID)
	if err != nil || report.Status != "finalizing" || report.Ballots != nil {
		t.Fatalf("closing report: %+v %v", report, err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO poll_results (poll_id,version,algorithm,ballots,counts)
		VALUES ($1,1,$2,0,'{"1":0,"2":0}')`, poll.ID, resultsrepo.AlgorithmVersion); err != nil {
		t.Fatal(err)
	}
	unfinished, err = repo.LoadUnfinished(ctx)
	if err != nil || len(unfinished) != 0 {
		t.Fatalf("discovered final poll: %v %v", unfinished, err)
	}
	report, err = repo.Results(ctx, poll.ID)
	if err != nil || report.Status != "final" || report.Ballots == nil || *report.Ballots != 0 ||
		report.Choices[0].Votes == nil || *report.Choices[0].Votes != 0 || report.Choices[0].Percent != nil {
		t.Fatalf("empty final report: %+v %v", report, err)
	}
}
