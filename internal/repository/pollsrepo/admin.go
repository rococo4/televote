package pollsrepo

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rococo4/televote/internal/entity"
	"github.com/rococo4/televote/internal/repository/resultsrepo"
)

//go:embed queries/results.sql
var resultsSQL string

// Draft reserves the immutable definition before Kafka is contacted. Concurrent
// requests for an ID see either the complete definition or a rolled back insert.
func (r *Repository) Draft(ctx context.Context, spec entity.CreatePoll, partitions int) (entity.Poll, bool, error) {
	poll := entity.Poll{
		ID: spec.ID, Version: 1, Topic: "televote-" + spec.ID.String(), Partitions: partitions,
		OpensAt: spec.OpensAt, ClosesAt: spec.ClosesAt, MinChoices: spec.MinChoices, MaxChoices: spec.MaxChoices,
		Choices: make(map[int]struct{}, len(spec.Choices)),
	}
	for i := range spec.Choices {
		poll.Choices[i+1] = struct{}{}
	}
	var published bool
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			INSERT INTO polls (id, version, question, opens_at, closes_at, min_choices, max_choices, topic, partitions)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (id) DO NOTHING`,
			poll.ID, poll.Version, spec.Question, poll.OpensAt, poll.ClosesAt, poll.MinChoices, poll.MaxChoices, poll.Topic, poll.Partitions)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 1 {
			tag, err = tx.Exec(ctx, `
				INSERT INTO poll_choices (poll_id, id, label)
				SELECT $1, ord, label FROM unnest($2::text[]) WITH ORDINALITY AS c(label, ord)
				WHERE statement_timestamp() < $3::timestamptz`, poll.ID, spec.Choices, poll.ClosesAt)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				return fmt.Errorf("%w: voting window already ended", entity.ErrInvalidPoll)
			}
			return nil
		}

		// The conflict may have waited for another transaction. This next
		// statement uses a fresh READ COMMITTED snapshot including its choices.
		// Retain the original partition count across configuration changes.
		var same bool
		err = tx.QueryRow(ctx, `
			SELECT p.published, p.partitions,
			       p.version=$2 AND p.question=$3 AND p.opens_at=$4 AND p.closes_at=$5
			       AND p.min_choices=$6 AND p.max_choices=$7 AND p.topic=$8
			       AND ARRAY(SELECT c.label FROM poll_choices c WHERE c.poll_id=p.id ORDER BY c.id)=$9::text[]
			       AND ARRAY(SELECT c.id FROM poll_choices c WHERE c.poll_id=p.id ORDER BY c.id)
			           = ARRAY(SELECT generate_series(1,cardinality($9::text[])))
			FROM polls p WHERE p.id=$1`,
			poll.ID, poll.Version, spec.Question, poll.OpensAt, poll.ClosesAt, poll.MinChoices, poll.MaxChoices,
			poll.Topic, spec.Choices).Scan(&published, &poll.Partitions, &same)
		if err != nil {
			return err
		}
		if !same {
			return entity.ErrPollConflict
		}
		return nil
	})
	if err != nil {
		return entity.Poll{}, false, fmt.Errorf("reserve poll %s: %w", poll.ID, err)
	}
	return poll, published, nil
}

func (r *Repository) Publish(ctx context.Context, poll entity.Poll, topicID uuid.UUID) error {
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, "UPDATE polls SET published=true WHERE id=$1 AND version=$2", poll.ID, poll.Version)
		if err != nil {
			return fmt.Errorf("publish poll %s: %w", poll.ID, err)
		}
		if tag.RowsAffected() != 1 {
			return entity.ErrPollConflict
		}
		return resultsrepo.BindTopic(ctx, tx, poll, topicID)
	})
}

func (r *Repository) Results(ctx context.Context, id uuid.UUID) (entity.PollReport, error) {
	rows, err := r.pool.Query(ctx, resultsSQL, id, resultsrepo.AlgorithmVersion)
	if err != nil {
		return entity.PollReport{}, err
	}
	defer rows.Close()
	var report entity.PollReport
	for rows.Next() {
		var choice entity.ChoiceResult
		if err := rows.Scan(&report.ID, &report.Question, &report.Status, &report.Ballots, &choice.ID, &choice.Label, &choice.Votes); err != nil {
			return entity.PollReport{}, err
		}
		if report.Ballots != nil {
			if choice.Votes == nil {
				return entity.PollReport{}, fmt.Errorf("poll %s final result lacks choice %d", id, choice.ID)
			}
			if *report.Ballots > 0 {
				percent := 100 * float64(*choice.Votes) / float64(*report.Ballots)
				choice.Percent = &percent
			}
		}
		report.Choices = append(report.Choices, choice)
	}
	if err := rows.Err(); err != nil {
		return entity.PollReport{}, err
	}
	if len(report.Choices) == 0 {
		return entity.PollReport{}, entity.ErrPollNotFound
	}
	return report, nil
}
