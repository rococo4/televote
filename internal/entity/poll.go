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
	Question   string
	Options    []PollChoice
	OpensAt    time.Time
	ClosesAt   time.Time
	MinChoices int
	MaxChoices int
	Choices    map[int]struct{}
	Topic      string
	Partitions int
}

type PollChoice struct {
	ID    int    `json:"id"`
	Label string `json:"label"`
}

type PublicPoll struct {
	ID         uuid.UUID    `json:"id"`
	Question   string       `json:"question"`
	Choices    []PollChoice `json:"choices"`
	OpensAt    time.Time    `json:"opens_at"`
	ClosesAt   time.Time    `json:"closes_at"`
	MinChoices int          `json:"min_choices"`
	MaxChoices int          `json:"max_choices"`
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
