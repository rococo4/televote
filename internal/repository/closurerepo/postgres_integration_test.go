//go:build integration

package closurerepo_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rococo4/televote/internal/entity"
	"github.com/rococo4/televote/internal/repository/closurerepo"
)

func TestPollClosureRegistration(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
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
	repo := closurerepo.New(pool)
	newPoll := func(t *testing.T, deadline string) entity.Poll {
		t.Helper()
		p := entity.Poll{ID: uuid.New(), Version: 9}
		_, err := pool.Exec(ctx, `INSERT INTO polls
			(id, version, question, opens_at, closes_at, min_choices, max_choices, topic, partitions, published)
			VALUES ($1,$2,'A/B',now()-interval '2 hours',now()+$3::text::interval,1,1,$4,1,true)`,
			p.ID, p.Version, deadline, p.ID.String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := pool.Exec(ctx, "DELETE FROM polls WHERE id=$1", p.ID); err != nil {
				t.Error(err)
			}
		})
		return p
	}

	t.Run("open", func(t *testing.T) {
		p, writer := newPoll(t, "1 hour"), uuid.New()
		for range 2 {
			if joined, err := repo.Join(ctx, p, writer); err != nil || !joined {
				t.Fatalf("join active writer: joined=%v err=%v", joined, err)
			}
		}
		for range 2 {
			if err := repo.Drain(ctx, p, writer); err != nil {
				t.Fatal(err)
			}
		}
		if joined, err := repo.Join(ctx, p, writer); err != nil || joined {
			t.Fatalf("rejoin drained writer: joined=%v err=%v", joined, err)
		}
		if decided, err := repo.Decide(ctx, p); err != nil || decided {
			t.Fatalf("close before deadline: decided=%v err=%v", decided, err)
		}
	})

	t.Run("legacy closed", func(t *testing.T) {
		p, writer := newPoll(t, "-1 hour"), uuid.New()
		// A pre-upgrade poll has no registered writer. It is eligible for
		// timeout once its stored deadline and the grace have both elapsed.
		if joined, err := repo.Join(ctx, p, writer); err != nil || joined {
			t.Fatalf("join expired poll: joined=%v err=%v", joined, err)
		}
		var frozen bool
		if err := pool.QueryRow(ctx, `SELECT frozen FROM poll_closures
			WHERE poll_id=$1 AND version=$2`, p.ID, p.Version).Scan(&frozen); err != nil || !frozen {
			t.Fatalf("expired roster: frozen=%v err=%v", frozen, err)
		}
		if err := repo.Drain(ctx, p, writer); err != nil {
			t.Fatal(err)
		}
		var initial time.Time
		for i := range 2 {
			if decided, err := repo.Decide(ctx, p); err != nil || !decided {
				t.Fatalf("close expired generation: decided=%v err=%v", decided, err)
			}
			var reason string
			var at time.Time
			if err := pool.QueryRow(ctx, `SELECT decision_reason, decided_at
				FROM poll_closures WHERE poll_id=$1 AND version=$2 AND frozen`, p.ID, p.Version).
				Scan(&reason, &at); err != nil {
				t.Fatal(err)
			}
			if reason != "timeout" || i > 0 && !at.Equal(initial) {
				t.Fatalf("decision changed on replay: %q at %s (first %s)", reason, at, initial)
			}
			initial = at
		}
	})
}
