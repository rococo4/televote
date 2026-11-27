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

type closureState struct {
	closesAt, cutoff time.Time
	frozen, decided  bool
}

// Join регистрирует реплику до приёма первого голоса по опросу.
// Все реплики получают из базы один срок ожидания отправок в Kafka.
func (r *Repository) Join(ctx context.Context, p entity.Poll, writer uuid.UUID) (bool, time.Time, error) {
	if writer == uuid.Nil {
		return false, time.Time{}, errors.New("empty writer ID")
	}
	return r.withLockedClosure(ctx, p, func(tx pgx.Tx, closure closureState) (bool, error) {
		if closure.frozen {
			return false, nil
		}
		tag, err := tx.Exec(ctx, `
			INSERT INTO poll_writers AS w (poll_id, writer_id)
			SELECT c.poll_id, $2
			FROM poll_closures c JOIN polls p ON p.id = c.poll_id
			WHERE c.poll_id = $1 AND p.published
			  AND clock_timestamp() < c.closes_at
			ON CONFLICT (poll_id, writer_id) DO UPDATE SET writer_id = w.writer_id
			    WHERE w.drained_at IS NULL`, p.ID, writer)
		if err != nil {
			return false, err
		}
		if tag.RowsAffected() == 1 {
			return true, nil
		}
		// Если срок голосования истёк, закрываем регистрацию реплик.
		// Даже если часы переведут назад, список останется закрытым.
		_, err = tx.Exec(ctx, `UPDATE poll_closures SET frozen = true
			WHERE poll_id = $1 AND NOT frozen
			  AND clock_timestamp() >= closes_at`, p.ID)
		return false, err
	})
}

// Drain отмечает, что реплика завершила обработку принятых запросов и отправку в Kafka.
// Повторный вызов сохраняет время первой отметки.
func (r *Repository) Drain(ctx context.Context, p entity.Poll, writer uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `
		WITH gate AS (
			SELECT poll_id FROM poll_closures
			WHERE poll_id = $1 FOR UPDATE
		)
		UPDATE poll_writers w SET drained_at = clock_timestamp()
		FROM gate WHERE w.poll_id = gate.poll_id
		  AND w.writer_id = $2 AND w.drained_at IS NULL`, p.ID, writer)
	return err
}

// Decide разрешает закрытие, когда все писатели отметились в срок или истекло
// время ожидания. Возвращает сохранённый для опроса срок, чтобы реплика могла
// дождаться его без повторных запросов к базе.
func (r *Repository) Decide(ctx context.Context, p entity.Poll) (ready bool, cutoff time.Time, err error) {
	return r.withLockedClosure(ctx, p, func(tx pgx.Tx, closure closureState) (bool, error) {
		if closure.decided {
			return true, nil
		}

		var now time.Time
		var drainedInTime bool
		err := tx.QueryRow(ctx, `
			SELECT clock_timestamp(), count(*) > 0 AND count(*) FILTER (
			    WHERE drained_at IS NULL OR drained_at > $2
			) = 0
			FROM poll_writers WHERE poll_id = $1`, p.ID, closure.cutoff).
			Scan(&now, &drainedInTime)
		if err != nil || now.Before(closure.closesAt) {
			return false, err
		}

		var reason string
		switch {
		case drainedInTime:
			reason = "drained"
		case !now.Before(closure.cutoff):
			reason = "timeout"
		default:
			if !closure.frozen {
				_, err = tx.Exec(ctx, `UPDATE poll_closures SET frozen = true
					WHERE poll_id = $1`, p.ID)
			}
			return false, err
		}
		_, err = tx.Exec(ctx, `UPDATE poll_closures
			SET frozen = true, decision_reason = $2, decided_at = $3
			WHERE poll_id = $1`, p.ID, reason, now)
		return true, err
	})
}

// Регистрация реплики и закрытие опроса блокируют строку закрытия одного опроса.
// При READ COMMITTED запрос после захвата блокировки увидит и те реплики,
// которые успели зарегистрироваться, пока транзакция ждала.
func (r *Repository) withLockedClosure(ctx context.Context, p entity.Poll, action func(pgx.Tx, closureState) (bool, error)) (bool, time.Time, error) {
	var result bool
	var closure closureState
	err := pgx.BeginTxFunc(ctx, r.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO poll_closures (poll_id, closes_at, grace_us)
			SELECT id, closes_at, $2 FROM polls WHERE id = $1 AND published
			ON CONFLICT (poll_id) DO NOTHING`, p.ID, r.grace.Microseconds())
		if err != nil {
			return err
		}
		var graceUs int64
		err = tx.QueryRow(ctx, `SELECT closes_at, grace_us, frozen, decided_at IS NOT NULL
			FROM poll_closures WHERE poll_id = $1 FOR UPDATE`, p.ID).
			Scan(&closure.closesAt, &graceUs, &closure.frozen, &closure.decided)
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.New("unknown published poll")
		}
		if err != nil {
			return err
		}
		closure.cutoff = closure.closesAt.Add(time.Duration(graceUs) * time.Microsecond)
		result, err = action(tx, closure)
		return err
	})
	if err != nil {
		return false, time.Time{}, fmt.Errorf("coordinate close for poll %s: %w", p.ID, err)
	}
	return result, closure.cutoff, nil
}
