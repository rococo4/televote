package closurerepo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rococo4/televote/internal/entity"
)

type Repository struct {
	pool  *pgxpool.Pool
	grace time.Duration
}

func New(pool *pgxpool.Pool, grace time.Duration) *Repository {
	return &Repository{pool: pool, grace: grace}
}

type generation struct {
	closesAt, cutoff time.Time
	frozen, decided  bool
}

// Join must commit before the process admits its first vote for this generation.
// The returned cutoff is shared by all writers regardless of their local config.
func (r *Repository) Join(ctx context.Context, p entity.Poll, writer uuid.UUID) (bool, time.Time, error) {
	if writer == uuid.Nil {
		return false, time.Time{}, errors.New("empty writer ID")
	}
	return r.withGeneration(ctx, p, func(tx pgx.Tx, g generation) (bool, error) {
		if g.frozen {
			return false, nil
		}
		tag, err := tx.Exec(ctx, `
			INSERT INTO poll_writers AS w (poll_id, version, writer_id)
			SELECT c.poll_id, c.version, $3
			FROM poll_closures c JOIN polls p ON (p.id, p.version) = (c.poll_id, c.version)
			WHERE c.poll_id = $1 AND c.version = $2 AND p.published
			  AND clock_timestamp() < c.closes_at
			ON CONFLICT (poll_id, version, writer_id) DO UPDATE SET writer_id = w.writer_id
			    WHERE w.drained_at IS NULL`, p.ID, p.Version, writer)
		if err != nil {
			return false, err
		}
		if tag.RowsAffected() == 1 {
			return true, nil
		}
		// Latch an observed deadline even when a late Join is the first
		// operation to see it. A later wall-clock correction cannot reopen it.
		_, err = tx.Exec(ctx, `UPDATE poll_closures SET frozen = true
			WHERE poll_id = $1 AND version = $2 AND NOT frozen
			  AND clock_timestamp() >= closes_at`, p.ID, p.Version)
		return false, err
	})
}

// Drain is a durable, idempotent acknowledgement of local admission and Kafka delivery drain.
func (r *Repository) Drain(ctx context.Context, p entity.Poll, writer uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `
		WITH gate AS (
			SELECT poll_id, version FROM poll_closures
			WHERE poll_id = $1 AND version = $2 FOR UPDATE
		)
		UPDATE poll_writers w SET drained_at = clock_timestamp()
		FROM gate WHERE (w.poll_id, w.version) = (gate.poll_id, gate.version)
		  AND w.writer_id = $3 AND w.drained_at IS NULL`, p.ID, p.Version, writer)
	return err
}

// Decide freezes the roster at the deadline. Once eligible for markers a
// generation stays eligible, including after a Kafka error or a process restart.
func (r *Repository) Decide(ctx context.Context, p entity.Poll) (bool, error) {
	ready, _, err := r.withGeneration(ctx, p, func(tx pgx.Tx, g generation) (bool, error) {
		if g.decided {
			return true, nil
		}

		var now time.Time
		var drainedInTime bool
		err := tx.QueryRow(ctx, `
			SELECT clock_timestamp(), count(*) > 0 AND count(*) FILTER (
			    WHERE drained_at IS NULL OR drained_at > $3
			) = 0
			FROM poll_writers WHERE poll_id = $1 AND version = $2`, p.ID, p.Version, g.cutoff).
			Scan(&now, &drainedInTime)
		if err != nil || now.Before(g.closesAt) {
			return false, err
		}

		var reason string
		switch {
		case drainedInTime:
			reason = "drained"
		case !now.Before(g.cutoff):
			reason = "timeout"
		default:
			if !g.frozen {
				_, err = tx.Exec(ctx, `UPDATE poll_closures SET frozen = true
					WHERE poll_id = $1 AND version = $2`, p.ID, p.Version)
			}
			return false, err
		}
		_, err = tx.Exec(ctx, `UPDATE poll_closures
			SET frozen = true, decision_reason = $3, decided_at = $4
			WHERE poll_id = $1 AND version = $2`, p.ID, p.Version, reason, now)
		return true, err
	})
	return ready, err
}

// Every join and decision locks the same generation row first. At READ COMMITTED
// the following statement sees writers that committed while acquiring the lock.
func (r *Repository) withGeneration(ctx context.Context, p entity.Poll, action func(pgx.Tx, generation) (bool, error)) (bool, time.Time, error) {
	var result bool
	var g generation
	err := pgx.BeginTxFunc(ctx, r.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO poll_closures (poll_id, version, closes_at, grace_us)
			SELECT id, version, closes_at, $3 FROM polls WHERE id = $1 AND version = $2 AND published
			ON CONFLICT (poll_id, version) DO NOTHING`, p.ID, p.Version, r.grace.Microseconds())
		if err != nil {
			return err
		}
		var graceUs int64
		err = tx.QueryRow(ctx, `SELECT closes_at, grace_us, frozen, decided_at IS NOT NULL
			FROM poll_closures WHERE poll_id = $1 AND version = $2 FOR UPDATE`, p.ID, p.Version).
			Scan(&g.closesAt, &graceUs, &g.frozen, &g.decided)
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.New("unknown published poll generation")
		}
		if err != nil {
			return err
		}
		g.cutoff = g.closesAt.Add(time.Duration(graceUs) * time.Microsecond)
		result, err = action(tx, g)
		return err
	})
	if err != nil {
		return false, time.Time{}, fmt.Errorf("coordinate close for poll %s version %d: %w", p.ID, p.Version, err)
	}
	return result, g.cutoff, nil
}
