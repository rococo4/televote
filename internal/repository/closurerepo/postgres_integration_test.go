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
	const originalGrace = 5 * time.Second
	repo := closurerepo.New(pool, originalGrace)
	newPoll := func(t *testing.T, deadline string) entity.Poll {
		t.Helper()
		p := entity.Poll{ID: uuid.New(), Version: 9}
		err := pool.QueryRow(ctx, `INSERT INTO polls
			(id, version, question, opens_at, closes_at, min_choices, max_choices, topic, partitions, published)
			VALUES ($1,$2,'A/B',now()-interval '2 hours',now()+$3::text::interval,1,1,$4,1,true)
			RETURNING closes_at`, p.ID, p.Version, deadline, p.ID.String()).Scan(&p.ClosesAt)
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
		// Reusing this generation with different settings must preserve its cutoff.
		for _, peer := range []*closurerepo.Repository{repo, closurerepo.New(pool, 0)} {
			if joined, cutoff, err := peer.Join(ctx, p, writer); err != nil || !joined || !cutoff.Equal(p.ClosesAt.Add(originalGrace)) {
				t.Fatalf("join active writer: joined=%v cutoff=%s err=%v", joined, cutoff, err)
			}
		}
		for range 2 {
			if err := repo.Drain(ctx, p, writer); err != nil {
				t.Fatal(err)
			}
		}
		if joined, _, err := repo.Join(ctx, p, writer); err != nil || joined {
			t.Fatalf("rejoin drained writer: joined=%v err=%v", joined, err)
		}
		if decided, err := repo.Decide(ctx, p); err != nil || decided {
			t.Fatalf("close before deadline: decided=%v err=%v", decided, err)
		}
	})

	t.Run("legacy closed", func(t *testing.T) {
		p, writer := newPoll(t, "-1 hour"), uuid.New()
		// A pre-upgrade writer inserted no explicit grace; the historical five
		// seconds must still apply to replicas with different local settings.
		if _, err := pool.Exec(ctx, `INSERT INTO poll_closures (poll_id, version, closes_at)
			VALUES ($1, $2, $3)`, p.ID, p.Version, p.ClosesAt); err != nil {
			t.Fatal(err)
		}
		peer := closurerepo.New(pool, 23*time.Hour)
		if joined, cutoff, err := peer.Join(ctx, p, writer); err != nil || joined || !cutoff.Equal(p.ClosesAt.Add(originalGrace)) {
			t.Fatalf("join expired poll: joined=%v cutoff=%s err=%v", joined, cutoff, err)
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
			if decided, err := peer.Decide(ctx, p); err != nil || !decided {
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

	t.Run("new generation gets its own grace", func(t *testing.T) {
		p := newPoll(t, "-1 hour")
		const longGrace = 2*time.Hour + time.Microsecond
		// No writer joined before close. The first decider creates the row.
		if ready, err := closurerepo.New(pool, longGrace).Decide(ctx, p); err != nil || ready {
			t.Fatalf("empty roster with long grace: ready=%v err=%v", ready, err)
		}
		immediate := closurerepo.New(pool, 0)
		if ready, err := immediate.Decide(ctx, p); err != nil || ready {
			t.Fatalf("restarted decider shortened existing grace: ready=%v err=%v", ready, err)
		}
		if _, cutoff, err := immediate.Join(ctx, p, uuid.New()); err != nil || !cutoff.Equal(p.ClosesAt.Add(longGrace)) {
			t.Fatalf("late join got cutoff %s, err=%v", cutoff, err)
		}
		p.Version++
		if _, err := pool.Exec(ctx, "UPDATE polls SET version=$2 WHERE id=$1", p.ID, p.Version); err != nil {
			t.Fatal(err)
		}
		if ready, err := immediate.Decide(ctx, p); err != nil || !ready {
			t.Fatalf("new generation with zero grace: ready=%v err=%v", ready, err)
		}
	})
}
