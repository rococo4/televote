package entity

import (
	"testing"
	"time"
)

func TestPollWindow(t *testing.T) {
	start := time.Unix(1_000, 0)
	p := Poll{OpensAt: start, ClosesAt: start.Add(time.Minute)}
	for _, tc := range []struct {
		at   time.Time
		want error
	}{
		{start.Add(-time.Nanosecond), ErrNotOpen},
		{start, nil},
		{p.ClosesAt.Add(-time.Nanosecond), nil},
		{p.ClosesAt, ErrClosed},
	} {
		if err := p.CheckWindow(tc.at); err != tc.want {
			t.Errorf("at %s: got %v, want %v", tc.at, err, tc.want)
		}
	}
}

func TestValidateBallot(t *testing.T) {
	p := Poll{MinChoices: 1, MaxChoices: 2, Choices: map[int]struct{}{0: {}, 1: {}, 2: {}}}
	for _, tc := range []struct {
		choices []int
		want    error
	}{
		{[]int{0}, nil},
		{[]int{2, 0}, nil},
		{nil, ErrInvalidBallot},
		{[]int{2, 2}, ErrInvalidBallot},
		{[]int{3}, ErrInvalidBallot},
		{[]int{0, 1, 2}, ErrInvalidBallot},
	} {
		if err := p.ValidateBallot(Ballot{Choices: tc.choices}); err != tc.want {
			t.Errorf("choices %v: got %v, want %v", tc.choices, err, tc.want)
		}
	}
}
