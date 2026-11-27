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
	f := newPollFixture(t, ctx, 5*time.Second, testCloseGrace, 2, []string{"A", "B", "C"})
	// Закрыть опрос должен тот же фоновый обработчик, который работает вместе с HTTP-сервером.
	startPollWorker(t, ctx, f.application)

	p := f.poll
	path := f.server.URL + "/api/polls/" + p.ID.String() + "/votes"
	token := uuid.New()
	tokenHex := hex.EncodeToString(token[:])
	postVote(t, ctx, f.server.Client(), path, tokenHex, 1, 2)
	postVote(t, ctx, f.server.Client(), path, tokenHex, 2, 3)

	// Зритель отправил допустимый вариант ровно в closes_at. В этот момент приём уже закрыт,
	// поэтому в итоговом бюллетене должны остаться B и C.
	err := f.writer.Publish(ctx, p, entity.VoteSubmitted{
		PollID: p.ID, VoterKey: entity.VoterKey(p.ID, token),
		ReceivedAt: p.ClosesAt, Choices: []int{1},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForFinal(t, ctx, f)
	var n int64
	var counts map[int]int64
	if err := f.pool.QueryRow(ctx, `SELECT ballots, counts FROM poll_results
		WHERE poll_id = $1`, p.ID).Scan(&n, &counts); err != nil {
		t.Fatal(err)
	}
	if n != 1 || !maps.Equal(counts, map[int]int64{1: 0, 2: 1, 3: 1}) {
		t.Fatalf("final: ballots=%d counts=%v, want 1 ballot A=0 B=1 C=1", n, counts)
	}
	status, reply := sendBallot(t, ctx, f.server.Client(), path, tokenHex, []int{1})
	if status != http.StatusBadRequest || reply["code"] != "bad_request" || reply["message"] != "Опрос закрыт." {
		t.Fatalf("POST after automatic close: HTTP %d, %v", status, reply)
	}
	t.Log("deadline scheduler persisted 1 ballot with A=0 B=1 C=1; late POST returned HTTP 400 (bad_request)")
}
