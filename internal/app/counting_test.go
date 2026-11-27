package app

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/rococo4/televote/internal/entity"
	"github.com/rococo4/televote/internal/service/counting"
	"github.com/rococo4/televote/internal/service/voting"
)

type storeCall struct {
	method string
	after  time.Duration
}

type recordedClosures struct {
	closureStore
	closesAt time.Time
	cutoff   time.Time
	calls    []storeCall
}

func (r *recordedClosures) record(method string) {
	r.calls = append(r.calls, storeCall{method, time.Since(r.closesAt)})
}

func (r *recordedClosures) Drain(context.Context, entity.Poll, uuid.UUID) error {
	r.record("Drain")
	return nil
}

func (r *recordedClosures) Decide(context.Context, entity.Poll) (bool, time.Time, error) {
	r.record("Decide")
	return !time.Now().Before(r.cutoff), r.cutoff, nil
}

type delayedVoteDelivery struct{ VoteJournal }

func (delayedVoteDelivery) WaitVotes(context.Context, uuid.UUID) error {
	time.Sleep(time.Second)
	return nil
}

type peerResults struct {
	counting.Results
	store *recordedClosures
}

func (r peerResults) Progress(context.Context, entity.Poll) (bool, map[int32]bool, error) {
	r.store.record("Progress")
	return !time.Now().Before(r.store.cutoff), nil, nil
}

func TestCountClosedPollWaitsForSharedDeadline(t *testing.T) {
	const grace = 5 * time.Second
	for _, tc := range []struct {
		name               string
		joined             bool
		startDelay         time.Duration
		cancelWhileWaiting bool
		wantCalls          []storeCall
	}{
		{
			name: "writer", joined: true,
			wantCalls: []storeCall{{"Drain", time.Second}, {"Decide", time.Second}, {"Decide", grace}, {"Progress", grace}},
		},
		{
			name: "started after close", startDelay: 3 * time.Second,
			wantCalls: []storeCall{{"Progress", 3 * time.Second}, {"Decide", 3 * time.Second}, {"Decide", grace}, {"Progress", grace}},
		},
		{
			name: "canceled", joined: true, cancelWhileWaiting: true,
			wantCalls: []storeCall{{"Drain", time.Second}, {"Decide", time.Second}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				poll := entity.Poll{ID: uuid.New(), ClosesAt: time.Now()}
				cutoff := poll.ClosesAt.Add(grace)
				store := &recordedClosures{closesAt: poll.ClosesAt, cutoff: cutoff}
				local := localPoll{poll: poll, joined: tc.joined}
				if tc.joined {
					local.drainDeadline = cutoff
				}
				application := App{
					voting:   voting.New([]entity.Poll{poll}, nil, time.Now),
					counting: counting.New(nil, peerResults{store: store}),
					polls:    map[uuid.UUID]localPoll{poll.ID: local},
					closures: store, journal: delayedVoteDelivery{},
				}

				time.Sleep(tc.startDelay)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if tc.cancelWhileWaiting {
					time.AfterFunc(2*time.Second, cancel)
				}
				err := application.CountClosedPoll(ctx, poll.ID)
				if tc.cancelWhileWaiting {
					if !errors.Is(err, context.Canceled) || time.Since(poll.ClosesAt) != 2*time.Second {
						t.Fatalf("отмена во время ожидания: err=%v, прошло %s", err, time.Since(poll.ClosesAt))
					}
				} else if err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(store.calls, tc.wantCalls) {
					t.Errorf("обращения к базе после конца эфира: %v, ожидали %v", store.calls, tc.wantCalls)
				}
			})
		})
	}
}
