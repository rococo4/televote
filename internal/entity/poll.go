package entity

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrNotOpen       = errors.New("poll has not opened")
	ErrClosed        = errors.New("poll is closed")
	ErrInvalidBallot = errors.New("invalid ballot")
)

type Poll struct {
	ID         uuid.UUID
	Version    int64
	OpensAt    time.Time
	ClosesAt   time.Time
	MinChoices int
	MaxChoices int
	Choices    map[int]struct{}
	Topic      string
	Partitions int
}

func (p Poll) CheckWindow(at time.Time) error {
	if at.Before(p.OpensAt) {
		return ErrNotOpen
	}
	if !at.Before(p.ClosesAt) {
		return ErrClosed
	}
	return nil
}

func (p Poll) ValidateBallot(b Ballot) error {
	if len(b.Choices) == 0 || len(b.Choices) < p.MinChoices || len(b.Choices) > p.MaxChoices {
		return ErrInvalidBallot
	}
	seen := make(map[int]struct{}, len(b.Choices))
	for _, id := range b.Choices {
		if _, allowed := p.Choices[id]; !allowed {
			return ErrInvalidBallot
		}
		if _, duplicate := seen[id]; duplicate {
			return ErrInvalidBallot
		}
		seen[id] = struct{}{}
	}
	return nil
}
