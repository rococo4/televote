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
		p := entity.Poll{ID: uuid.New()}
		err := pool.QueryRow(ctx, `INSERT INTO polls
			(id, question, opens_at, closes_at, min_choices, max_choices, topic, partitions, published)
			VALUES ($1,'A/B',now()-interval '2 hours',now()+$2::text::interval,1,1,$3,1,true)
			RETURNING closes_at`, p.ID, deadline, p.ID.String()).Scan(&p.ClosesAt)
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
		// Вторая реплика с нулевым тайм-аутом должна получить срок ожидания первой.
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
		if ready, cutoff, err := repo.Decide(ctx, p); err != nil || ready || !cutoff.Equal(p.ClosesAt.Add(originalGrace)) {
			t.Fatalf("close before deadline: ready=%v cutoff=%s err=%v", ready, cutoff, err)
		}
	})

	t.Run("closed without writers", func(t *testing.T) {
		p, writer := newPoll(t, "-1 hour"), uuid.New()
		// При создании записи без явного срока база подставляет пять секунд.
		// Даже с другой настройкой реплика должна взять срок из базы.
		if _, err := pool.Exec(ctx, `INSERT INTO poll_closures (poll_id, closes_at)
			VALUES ($1, $2)`, p.ID, p.ClosesAt); err != nil {
			t.Fatal(err)
		}
		peer := closurerepo.New(pool, 23*time.Hour)
		if joined, cutoff, err := peer.Join(ctx, p, writer); err != nil || joined || !cutoff.Equal(p.ClosesAt.Add(originalGrace)) {
			t.Fatalf("join expired poll: joined=%v cutoff=%s err=%v", joined, cutoff, err)
		}
		var frozen bool
		if err := pool.QueryRow(ctx, `SELECT frozen FROM poll_closures
			WHERE poll_id=$1`, p.ID).Scan(&frozen); err != nil || !frozen {
			t.Fatalf("expired roster: frozen=%v err=%v", frozen, err)
		}
		if err := repo.Drain(ctx, p, writer); err != nil {
			t.Fatal(err)
		}
		var initial time.Time
		for i := range 2 {
			if ready, cutoff, err := peer.Decide(ctx, p); err != nil || !ready || !cutoff.Equal(p.ClosesAt.Add(originalGrace)) {
				t.Fatalf("close expired poll: ready=%v cutoff=%s err=%v", ready, cutoff, err)
			}
			var reason string
			var at time.Time
			if err := pool.QueryRow(ctx, `SELECT decision_reason, decided_at
				FROM poll_closures WHERE poll_id=$1 AND frozen`, p.ID).
				Scan(&reason, &at); err != nil {
				t.Fatal(err)
			}
			if reason != "timeout" || i > 0 && !at.Equal(initial) {
				t.Fatalf("decision changed on replay: %q at %s (first %s)", reason, at, initial)
			}
			initial = at
		}
	})

	t.Run("new poll gets its own grace", func(t *testing.T) {
		p := newPoll(t, "-1 hour")
		const longGrace = 2*time.Hour + time.Microsecond
		// Пока опрос был открыт, ни одна реплика не зарегистрировалась.
		// Срок ожидания задаст та, которая первой вызовет Decide.
		wantCutoff := p.ClosesAt.Add(longGrace)
		if ready, cutoff, err := closurerepo.New(pool, longGrace).Decide(ctx, p); err != nil || ready || !cutoff.Equal(wantCutoff) {
			t.Fatalf("empty roster with long grace: ready=%v cutoff=%s err=%v", ready, cutoff, err)
		}
		immediate := closurerepo.New(pool, 0)
		if ready, cutoff, err := immediate.Decide(ctx, p); err != nil || ready || !cutoff.Equal(wantCutoff) {
			t.Fatalf("restarted decider changed existing grace: ready=%v cutoff=%s err=%v", ready, cutoff, err)
		}
		if _, cutoff, err := immediate.Join(ctx, p, uuid.New()); err != nil || !cutoff.Equal(p.ClosesAt.Add(longGrace)) {
			t.Fatalf("late join got cutoff %s, err=%v", cutoff, err)
		}
		next := newPoll(t, "-1 hour")
		if ready, cutoff, err := immediate.Decide(ctx, next); err != nil || !ready || !cutoff.Equal(next.ClosesAt) {
			t.Fatalf("new poll with zero grace: ready=%v cutoff=%s err=%v", ready, cutoff, err)
		}
	})
}
