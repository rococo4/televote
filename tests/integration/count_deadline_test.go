//go:build integration

package integration_test

import (
	"context"
	"encoding/hex"
	"maps"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/rococo4/televote/internal/entity"
)

func TestCountVotesOnDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	f := newPollFixture(t, ctx, 5*time.Second, 2, []string{"A", "B", "C"})
	// Start the same scheduler that runs alongside the HTTP server in cmd/televote.
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.handler.RunCounting(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	p := f.poll
	path := f.server.URL + "/api/polls/" + p.ID.String() + "/votes"
	token := uuid.New()
	tokenHex := hex.EncodeToString(token[:])
	postVote(t, ctx, f.server.Client(), path, tokenHex, 1, 2)
	postVote(t, ctx, f.server.Client(), path, tokenHex, 2, 3)

	// This later record has the same key and a valid selection, but its
	// server timestamp is outside [opens_at, closes_at). It must not replace B+C.
	err := f.writer.Publish(ctx, p, entity.VoteSubmitted{
		PollID: p.ID, Version: p.Version, VoterKey: entity.VoterKey(p.ID, token),
		ReceivedAt: p.ClosesAt, Choices: []int{1},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("no final result before test deadline")
	}
	var n int64
	var counts map[int]int64
	if err := f.pool.QueryRow(ctx, `SELECT ballots, counts FROM poll_results
		WHERE poll_id = $1 AND version = $2 AND algorithm = 1`, p.ID, p.Version).Scan(&n, &counts); err != nil {
		t.Fatal(err)
	}
	if n != 1 || !maps.Equal(counts, map[int]int64{1: 0, 2: 1, 3: 1}) {
		t.Fatalf("final: ballots=%d counts=%v, want 1 ballot A=0 B=1 C=1", n, counts)
	}
	status, reply := sendBallot(t, ctx, f.server.Client(), path, tokenHex, []int{1})
	if status != http.StatusGone || reply["code"] != "closed" {
		t.Fatalf("POST after automatic close: HTTP %d, %v", status, reply)
	}
	t.Log("deadline scheduler persisted 1 ballot with A=0 B=1 C=1; late POST returned HTTP 410")
}
