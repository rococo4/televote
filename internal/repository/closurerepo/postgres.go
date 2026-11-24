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

const GracePeriod = 5 * time.Second

type Repository struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Repository { return &Repository{pool: pool} }

// Join must commit before the process admits its first vote for this generation.
func (r *Repository) Join(ctx context.Context, p entity.Poll, writer uuid.UUID) (bool, error) {
	if writer == uuid.Nil {
		return false, errors.New("empty writer ID")
	}
	return r.withGeneration(ctx, p, func(tx pgx.Tx) (bool, error) {
		tag, err := tx.Exec(ctx, `
			INSERT INTO poll_writers AS w (poll_id, version, writer_id)
			SELECT c.poll_id, c.version, $3
			FROM poll_closures c JOIN polls p ON (p.id, p.version) = (c.poll_id, c.version)
			WHERE c.poll_id = $1 AND c.version = $2 AND p.published
			  AND NOT c.frozen AND clock_timestamp() < c.closes_at
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
	return r.withGeneration(ctx, p, func(tx pgx.Tx) (bool, error) {
		var closesAt, now time.Time
		var frozen, decided bool
		err := tx.QueryRow(ctx, `
			SELECT closes_at, clock_timestamp(), frozen, decided_at IS NOT NULL
			FROM poll_closures WHERE poll_id = $1 AND version = $2`, p.ID, p.Version).
			Scan(&closesAt, &now, &frozen, &decided)
		if err != nil || decided || now.Before(closesAt) {
			return decided, err
		}

		var drainedInTime bool
		cutoff := closesAt.Add(GracePeriod)
		err = tx.QueryRow(ctx, `
			SELECT count(*) > 0 AND count(*) FILTER (
			    WHERE drained_at IS NULL OR drained_at > $3
			) = 0
			FROM poll_writers WHERE poll_id = $1 AND version = $2`, p.ID, p.Version, cutoff).
			Scan(&drainedInTime)
		if err != nil {
			return false, err
		}

		var reason string
		switch {
		case drainedInTime:
			reason = "drained"
		case !now.Before(cutoff):
			reason = "timeout"
		default:
			if !frozen {
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
}

// Every join and decision locks the same generation row first. At READ COMMITTED
// the following statement sees writers that committed while acquiring the lock.
func (r *Repository) withGeneration(ctx context.Context, p entity.Poll, action func(pgx.Tx) (bool, error)) (bool, error) {
	var result bool
	err := pgx.BeginTxFunc(ctx, r.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO poll_closures (poll_id, version, closes_at)
			SELECT id, version, closes_at FROM polls WHERE id = $1 AND version = $2 AND published
			ON CONFLICT (poll_id, version) DO NOTHING`, p.ID, p.Version)
		if err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `SELECT poll_id FROM poll_closures
			WHERE poll_id = $1 AND version = $2 FOR UPDATE`, p.ID, p.Version)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return errors.New("unknown published poll generation")
		}
		result, err = action(tx)
		return err
	})
	if err != nil {
		return false, fmt.Errorf("coordinate close for poll %s version %d: %w", p.ID, p.Version, err)
	}
	return result, nil
}
