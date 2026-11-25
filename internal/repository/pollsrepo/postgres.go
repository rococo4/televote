package pollsrepo

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rococo4/televote/internal/entity"
	"github.com/rococo4/televote/internal/repository/resultsrepo"
)

//go:embed queries/load_published.sql
var loadPublishedSQL string

type Repository struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) LoadPublished(ctx context.Context) ([]entity.Poll, error) {
	return r.load(ctx, loadPublishedSQL+" ORDER BY p.id")
}

func (r *Repository) LoadUnfinished(ctx context.Context) ([]entity.Poll, error) {
	return r.load(ctx, loadPublishedSQL+`
		AND NOT EXISTS (SELECT 1 FROM poll_results f
		                WHERE f.poll_id=p.id AND f.version=p.version AND f.algorithm=$1)
		ORDER BY p.id`, resultsrepo.AlgorithmVersion)
}

func (r *Repository) FindPublished(ctx context.Context, id uuid.UUID) (entity.Poll, error) {
	row := r.pool.QueryRow(ctx, loadPublishedSQL+" AND p.id=$1", id)
	poll, err := scanPoll(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return entity.Poll{}, entity.ErrPollNotFound
	}
	return poll, err
}

func (r *Repository) load(ctx context.Context, query string, args ...any) ([]entity.Poll, error) {
	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query published polls: %w", err)
	}
	defer rows.Close()

	var polls []entity.Poll
	for rows.Next() {
		poll, err := scanPoll(rows)
		if err != nil {
			return nil, err
		}
		polls = append(polls, poll)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read published polls: %w", err)
	}
	return polls, nil
}

func scanPoll(row pgx.Row) (entity.Poll, error) {
	var poll entity.Poll
	var choices []int32
	if err := row.Scan(&poll.ID, &poll.Version, &poll.OpensAt, &poll.ClosesAt,
		&poll.MinChoices, &poll.MaxChoices, &poll.Topic, &poll.Partitions, &choices); err != nil {
		return poll, fmt.Errorf("scan published poll: %w", err)
	}
	if poll.MaxChoices > len(choices) {
		return poll, fmt.Errorf("published poll %s permits %d choices but has %d", poll.ID, poll.MaxChoices, len(choices))
	}
	poll.Choices = make(map[int]struct{}, len(choices))
	for _, id := range choices {
		poll.Choices[int(id)] = struct{}{}
	}
	return poll, nil
}
