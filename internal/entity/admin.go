package entity

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidPoll  = errors.New("invalid poll")
	ErrPollConflict = errors.New("poll ID already has a different definition")
	ErrPollNotFound = errors.New("published poll not found")
)

type CreatePoll struct {
	ID                     uuid.UUID
	Question               string
	Choices                []string
	OpensAt, ClosesAt      time.Time
	MinChoices, MaxChoices int
}

// Normalize uses PostgreSQL timestamp precision so an identical retry compares equal.
func (p CreatePoll) Normalize() (CreatePoll, error) {
	p.OpensAt = p.OpensAt.UTC().Truncate(time.Microsecond)
	p.ClosesAt = p.ClosesAt.UTC().Truncate(time.Microsecond)
	p.Question = strings.TrimSpace(p.Question)
	if p.Question == "" || strings.IndexByte(p.Question, 0) >= 0 {
		return p, fmt.Errorf("%w: question is empty or contains NUL", ErrInvalidPoll)
	}
	if p.OpensAt.IsZero() || p.ClosesAt.IsZero() || !p.OpensAt.Before(p.ClosesAt) {
		return p, fmt.Errorf("%w: invalid voting window", ErrInvalidPoll)
	}
	if len(p.Choices) < 2 || p.MinChoices < 1 || p.MaxChoices < p.MinChoices || p.MaxChoices > len(p.Choices) {
		return p, fmt.Errorf("%w: invalid choices or selection limits", ErrInvalidPoll)
	}
	p.Choices = slices.Clone(p.Choices)
	seen := make(map[string]bool, len(p.Choices))
	for i, label := range p.Choices {
		label = strings.TrimSpace(label)
		if label == "" || strings.IndexByte(label, 0) >= 0 || seen[label] {
			return p, fmt.Errorf("%w: choice %d is empty, contains NUL or repeats a label", ErrInvalidPoll, i+1)
		}
		p.Choices[i] = label
		seen[label] = true
	}
	return p, nil
}

type PollReport struct {
	ID       uuid.UUID      `json:"id"`
	Question string         `json:"question"`
	Status   string         `json:"status"`
	Ballots  *int64         `json:"ballots,omitempty"`
	Choices  []ChoiceResult `json:"choices"`
}

type ChoiceResult struct {
	ID      int      `json:"id"`
	Label   string   `json:"label"`
	Votes   *int64   `json:"votes,omitempty"`
	Percent *float64 `json:"percent,omitempty"`
}

// Final reports explicitly represent an undefined percentage when no ballots were cast.
func (c ChoiceResult) MarshalJSON() ([]byte, error) {
	type choice ChoiceResult
	if c.Votes == nil {
		return json.Marshal(choice(c))
	}
	return json.Marshal(struct {
		choice
		Percent *float64 `json:"percent"`
	}{choice(c), c.Percent})
}
