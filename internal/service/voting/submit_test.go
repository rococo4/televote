package voting

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rococo4/televote/internal/entity"
)

type publishFunc func(context.Context, entity.Poll, entity.VoteSubmitted) error

func (f publishFunc) Publish(ctx context.Context, p entity.Poll, e entity.VoteSubmitted) error {
	return f(ctx, p, e)
}

func TestAdmittedVoteFinishesAfterClose(t *testing.T) {
	opens := time.Unix(100, 0).UTC()
	p := entity.Poll{
		ID: uuid.New(), Version: 5, OpensAt: opens, ClosesAt: opens.Add(time.Minute),
		Choices: map[int]struct{}{2: {}}, MinChoices: 1, MaxChoices: 1,
	}
	started := make(chan entity.VoteSubmitted, 1)
	ack := make(chan struct{})
	publish := publishFunc(func(ctx context.Context, _ entity.Poll, e entity.VoteSubmitted) error {
		started <- e
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ack:
			return nil
		}
	})
	var elapsed atomic.Int64
	elapsed.Store(int64(59*time.Second + 900*time.Millisecond))
	s := New([]entity.Poll{p}, publish, func() time.Time { return opens.Add(time.Duration(elapsed.Load())) })
	a, err := s.Begin(p.ID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // The browser disconnected after sending the body.
	done := make(chan error, 1)
	go func() {
		defer a.Finish()
		done <- s.Submit(ctx, a, [16]byte{1}, entity.Ballot{Choices: []int{2}})
	}()
	event := <-started
	if !event.ReceivedAt.Equal(p.ClosesAt.Add(-100*time.Millisecond)) || event.PollID != p.ID || event.Version != p.Version {
		t.Fatalf("wrong event metadata: %+v", event)
	}
	elapsed.Store(int64(60*time.Second + 100*time.Millisecond))
	if _, err := s.Begin(p.ID); !errors.Is(err, entity.ErrClosed) {
		t.Fatalf("late admission: got %v", err)
	}
	select {
	case err := <-done:
		t.Fatalf("publication returned before ACK: %v", err)
	default:
	}
	close(ack)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	s.Drain()
	if _, err := s.Begin(p.ID); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("registration after shutdown: %v", err)
	}
}
