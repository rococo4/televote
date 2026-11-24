package pollsrepo

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rococo4/televote/internal/entity"
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
	rows, err := r.pool.Query(ctx, loadPublishedSQL)
	if err != nil {
		return nil, fmt.Errorf("query published polls: %w", err)
	}
	defer rows.Close()

	var polls []entity.Poll
	for rows.Next() {
		var poll entity.Poll
		var choices []int32
		if err := rows.Scan(&poll.ID, &poll.Version, &poll.OpensAt, &poll.ClosesAt,
			&poll.MinChoices, &poll.MaxChoices, &poll.Topic, &poll.Partitions, &choices); err != nil {
			return nil, fmt.Errorf("scan published poll: %w", err)
		}
		if poll.MaxChoices > len(choices) {
			return nil, fmt.Errorf("published poll %s permits %d choices but has %d", poll.ID, poll.MaxChoices, len(choices))
		}
		poll.Choices = make(map[int]struct{}, len(choices))
		for _, id := range choices {
			poll.Choices[int(id)] = struct{}{}
		}
		polls = append(polls, poll)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read published polls: %w", err)
	}
	return polls, nil
}
