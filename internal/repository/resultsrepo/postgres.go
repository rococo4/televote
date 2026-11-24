package resultsrepo

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rococo4/televote/internal/entity"
)

// Algorithm 1 counts the last admissible ballot in partition order.
const algorithm = 1

//go:embed queries/save_partition.sql
var savePartitionSQL string

//go:embed queries/finalize.sql
var finalizeSQL string

type Repository struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Repository { return &Repository{pool: pool} }

func (r *Repository) Bind(ctx context.Context, p entity.Poll, topicID uuid.UUID) error {
	if topicID == uuid.Nil {
		return errors.New("empty Kafka topic ID")
	}
	tag, err := r.pool.Exec(ctx, `
		INSERT INTO poll_count_manifests AS m (poll_id, version, algorithm, topic, topic_id, partitions)
		SELECT id, version, $3, topic, $4, partitions FROM polls
		WHERE id = $1 AND version = $2 AND topic = $5 AND partitions = $6
		ON CONFLICT (poll_id, version, algorithm) DO UPDATE
		    SET topic_id = m.topic_id
		    WHERE (m.topic, m.topic_id, m.partitions) = (excluded.topic, excluded.topic_id, excluded.partitions)`,
		p.ID, p.Version, algorithm, topicID, p.Topic, p.Partitions)
	if err != nil {
		return fmt.Errorf("bind Kafka topic for poll %s: %w", p.ID, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("poll %s version %d configuration or bound Kafka topic changed", p.ID, p.Version)
	}
	return nil
}

func (r *Repository) BoundTopic(ctx context.Context, p entity.Poll) (uuid.UUID, bool, error) {
	var id uuid.UUID
	var name string
	var partitions int
	err := r.pool.QueryRow(ctx, `
		SELECT topic_id, topic, partitions FROM poll_count_manifests
		WHERE poll_id = $1 AND version = $2 AND algorithm = $3`,
		p.ID, p.Version, algorithm).Scan(&id, &name, &partitions)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("read bound Kafka topic: %w", err)
	}
	if name != p.Topic || partitions != p.Partitions {
		return uuid.Nil, false, fmt.Errorf("poll %s version %d differs from its manifest", p.ID, p.Version)
	}
	return id, true, nil
}

func (r *Repository) Progress(ctx context.Context, p entity.Poll) (bool, map[int32]bool, error) {
	var final bool
	var partitions []int32
	err := r.pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM poll_results
		               WHERE poll_id = $1 AND version = $2 AND algorithm = $3),
		       ARRAY(SELECT partition_id FROM poll_partition_results
		             WHERE poll_id = $1 AND version = $2 AND algorithm = $3)`,
		p.ID, p.Version, algorithm).Scan(&final, &partitions)
	if err != nil {
		return false, nil, fmt.Errorf("load counting progress: %w", err)
	}
	done := make(map[int32]bool, len(partitions))
	for _, id := range partitions {
		done[id] = true
	}
	return final, done, nil
}

func (r *Repository) SavePartition(ctx context.Context, p entity.Poll, result entity.PartitionResult) error {
	if len(result.Counts) != len(p.Choices) {
		return errors.New("partition result has a different set of choices")
	}
	for id := range p.Choices {
		n, ok := result.Counts[id]
		if !ok || n < 0 || n > result.Ballots {
			return fmt.Errorf("invalid count for choice %d", id)
		}
	}
	tag, err := r.pool.Exec(ctx, savePartitionSQL, p.ID, p.Version, algorithm, result.Partition,
		result.StartOffset, result.EndOffset, result.Ballots, result.Counts)
	if err != nil {
		return fmt.Errorf("save result for partition %d: %w", result.Partition, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("partition %d absent from manifest or replay differs from saved result", result.Partition)
	}
	return nil
}

func (r *Repository) Finalize(ctx context.Context, p entity.Poll) error {
	tag, err := r.pool.Exec(ctx, finalizeSQL, p.ID, p.Version, algorithm)
	if err != nil {
		return fmt.Errorf("save final result for poll %s: %w", p.ID, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("poll %s version %d has incomplete or conflicting partition results", p.ID, p.Version)
	}
	return nil
}
