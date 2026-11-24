package votes

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rococo4/televote/internal/entity"
	"github.com/rococo4/televote/internal/service/voting"
)

type recordingPublisher struct {
	events []entity.VoteSubmitted
	err    error
}

func (p *recordingPublisher) Publish(_ context.Context, _ entity.Poll, e entity.VoteSubmitted) error {
	p.events = append(p.events, e)
	return p.err
}

func TestPOST(t *testing.T) {
	pollID := uuid.MustParse("581fc419-006d-436d-9fc3-a8dd18744719")
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	poll := entity.Poll{
		ID: pollID, OpensAt: start, ClosesAt: start.Add(time.Minute),
		MinChoices: 1, MaxChoices: 2, Choices: map[int]struct{}{0: {}, 1: {}, 2: {}},
	}
	const body = `{"token":"00000000000000000000000000000000","choices":[2]}`
	id := pollID.String()
	for _, tc := range []struct {
		name       string
		pollID     string
		body       string
		sinceOpen  time.Duration
		status     int
		code       string
		publishErr error
	}{
		{"accepted", id, body, 0, 202, "accepted", nil},
		{"malformed JSON", id, body + "{}", 0, 400, "bad_request", nil},
		{"malformed UUID", "bad", body, 0, 400, "bad_request", nil},
		{"unpublished", uuid.Nil.String(), body, 0, 404, "poll_not_found", nil},
		{"not open", id, body, -1, 409, "not_open", nil},
		{"closed", id, body, time.Minute, 410, "closed", nil},
		{"invalid token", id, strings.Replace(body, "0000", "GGGG", 1), 0, 422, "invalid_ballot", nil},
		{"unknown choice", id, strings.Replace(body, "[2]", "[3]", 1), 0, 422, "invalid_ballot", nil},
		{"repeated choice", id, strings.Replace(body, "[2]", "[2,2]", 1), 0, 422, "invalid_ballot", nil},
		{"null choice", id, strings.Replace(body, "[2]", "[null]", 1), 0, 422, "invalid_ballot", nil},
		{"string choice", id, strings.Replace(body, "[2]", `["2"]`, 1), 0, 422, "invalid_ballot", nil},
		{"missing choices", id, `{"token":"00000000000000000000000000000000"}`, 0, 422, "invalid_ballot", nil},
		{"extra property", id, strings.Replace(body, "{", `{"extra":42,`, 1), 0, 422, "invalid_ballot", nil},
		{"unexpected casing", id, strings.Replace(body, "choices", "Choices", 1), 0, 422, "invalid_ballot", nil},
		{"oversize", id, strings.Repeat(" ", maxBodyBytes+1), 0, 413, "payload_too_large", nil},
		{"publish error", id, body, 0, 503, "unavailable", errors.New("no ACK")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			publisher := &recordingPublisher{err: tc.publishErr}
			s := voting.New([]entity.Poll{poll}, publisher, func() time.Time { return start.Add(tc.sinceOpen) })
			req := httptest.NewRequest(http.MethodPost, "/api/polls/"+tc.pollID+"/votes", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			NewHandler(s).ServeHTTP(w, req)
			if w.Code != tc.status || w.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("got %d %v: %s", w.Code, w.Header(), w.Body.String())
			}
			var result map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			key := "code"
			if tc.status == http.StatusAccepted {
				key = "status"
			}
			if result[key] != tc.code {
				t.Errorf("got %v, want %s=%s", result, key, tc.code)
			}
			wantEvents := 0
			if tc.status == http.StatusAccepted || tc.publishErr != nil {
				wantEvents = 1
			}
			if len(publisher.events) != wantEvents {
				t.Errorf("published %d events, want %d", len(publisher.events), wantEvents)
			}
		})
	}
}
