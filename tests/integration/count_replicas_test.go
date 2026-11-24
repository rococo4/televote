//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/rococo4/televote/internal/adapter/kafka"
	"github.com/rococo4/televote/internal/app"
	"github.com/rococo4/televote/internal/entity"
)

func TestCountVotesAcrossReplicas(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reason string
		status int
		a, b   int64
	}{
		{"all_writers_drained", "drained", http.StatusAccepted, 0, 1},
		{"missing_writer_and_late_delivery", "timeout", http.StatusServiceUnavailable, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			f := newVoteFixture(t, ctx, 3*time.Second)
			p := f.poll
			writer, err := kafka.New(f.brokers)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(writer.Close)
			gate := &delayedJournal{Producer: writer, ctx: ctx, admitted: make(chan struct{}), release: make(chan struct{}), delivered: make(chan struct{})}
			second, err := app.NewHandler(ctx, f.pool, gate)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(second)
			t.Cleanup(server.Close)

			// Same viewer: the result changes from A to B only if the second
			// replica appends its admitted vote before partition 0's first CLOSE.
			token := tokenForPartition(p, 0)
			path := "/api/polls/" + p.ID.String() + "/votes"
			postVote(t, ctx, f.server.Client(), f.server.URL+path, token, 1)
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+path,
				strings.NewReader(fmt.Sprintf(`{"token":%q,"choices":[2]}`, token)))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			status := make(chan int, 1)
			go func() {
				res, err := server.Client().Do(req)
				if err != nil {
					status <- 0
					return
				}
				res.Body.Close()
				status <- res.StatusCode
			}()
			select {
			case <-gate.admitted:
			case code := <-status:
				t.Fatalf("second POST returned %d before publishing", code)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

			done := startCounter(ctx, second)
			t.Cleanup(func() { cancel(); <-done })
			if tc.reason == "drained" {
				// A is ready; B still holds the last vote admitted before the deadline.
				doneA := startCounter(ctx, f.handler)
				t.Cleanup(func() { cancel(); <-doneA })
				waitPG(ctx, t, f, `SELECT count(*) = 1 FROM poll_writers
					WHERE poll_id = $1 AND drained_at IS NOT NULL`)
				assertPartitionEnds(t, ctx, f, []int64{1, 0, 0, 0})
				assertNoFinal(t, ctx, f)
				close(gate.release)
			}
			// In the timeout case A never reports and B is the only scheduler.
			// It must publish a final even with its OWN delivery callback stalled.
			waitPG(ctx, t, f, `SELECT EXISTS (SELECT 1 FROM poll_results WHERE poll_id=$1)`)
			var reason string
			var decidedAt, closesAt time.Time
			if err := f.pool.QueryRow(ctx, `SELECT decision_reason, decided_at, closes_at
				FROM poll_closures WHERE poll_id=$1 AND version=$2`, p.ID, p.Version).Scan(&reason, &decidedAt, &closesAt); err != nil {
				t.Fatal(err)
			}
			delay := decidedAt.Sub(closesAt) // Both timestamps came from PostgreSQL.
			if reason != tc.reason || delay < 0 || (delay >= 5*time.Second) != (tc.reason == "timeout") {
				t.Fatalf("decision=%s delay=%s; want %s", reason, delay, tc.reason)
			}
			if tc.reason == "timeout" {
				// Kafka may append a timed-out send. Releasing it only after FINAL
				// forces the ambiguous record strictly after the first CLOSE.
				close(gate.release)
			}
			select {
			case <-gate.delivered:
				if gate.err != nil {
					t.Fatal(gate.err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case code := <-status:
				if code != tc.status {
					t.Fatalf("second HTTP status %d, want %d", code, tc.status)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

			var ballots, a, b, end int64
			err = f.pool.QueryRow(ctx, `SELECT r.ballots,(r.counts->>'1')::bigint,(r.counts->>'2')::bigint,pr.end_offset
				FROM poll_results r JOIN poll_partition_results pr USING(poll_id,version,algorithm)
				WHERE r.poll_id=$1 AND pr.partition_id=0`, p.ID).Scan(&ballots, &a, &b, &end)
			if err != nil {
				t.Fatal(err)
			}
			if ballots != 1 || a != tc.a || b != tc.b {
				t.Fatalf("final ballots=%d A=%d B=%d, want 1/%d/%d", ballots, a, b, tc.a, tc.b)
			}
			assertLastVoteOffset(t, ctx, f, end, tc.reason == "timeout")
			t.Logf("decision=%s (%s after deadline), second POST=%d, CLOSE offset=%d, final A=%d B=%d", reason, delay, tc.status, end, a, b)
		})
	}
}

func startCounter(ctx context.Context, h *app.Handler) <-chan struct{} {
	done := make(chan struct{})
	go func() { defer close(done); h.RunCounting(ctx) }()
	return done
}

// Emulate a Kafka send with an ambiguous outcome on a real producer. The caller
// can time out first; WaitVotes only succeeds when the real delivery settles.
type delayedJournal struct {
	*kafka.Producer
	ctx                          context.Context
	admitted, release, delivered chan struct{}
	err                          error // read only after delivered closes
}

func (j *delayedJournal) Publish(ctx context.Context, p entity.Poll, v entity.VoteSubmitted) error {
	close(j.admitted)
	go func() {
		defer close(j.delivered)
		select {
		case <-j.release:
			j.err = j.Producer.Publish(j.ctx, p, v)
		case <-j.ctx.Done():
			j.err = j.ctx.Err()
		}
	}()
	select {
	case <-j.delivered:
		return j.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (j *delayedJournal) WaitVotes(ctx context.Context, id uuid.UUID) error {
	select {
	case <-j.delivered:
		return j.Producer.WaitVotes(ctx, id)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitPG(ctx context.Context, t *testing.T, f *voteFixture, q string) {
	t.Helper()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		var ok bool
		if err := f.pool.QueryRow(ctx, q, f.poll.ID).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		if ok {
			return
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatalf("timeout waiting for %s: %v", q, ctx.Err())
		}
	}
}

func assertNoFinal(t *testing.T, ctx context.Context, f *voteFixture) {
	t.Helper()
	var exists bool
	if err := f.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM poll_results WHERE poll_id=$1)`, f.poll.ID).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("premature final while B still has an admitted vote")
	}
}

func assertPartitionEnds(t *testing.T, ctx context.Context, f *voteFixture, want []int64) {
	t.Helper()
	request := kmsg.NewPtrListOffsetsRequest()
	topic := kmsg.ListOffsetsRequestTopic{Topic: f.poll.Topic}
	for i := range want {
		p := kmsg.NewListOffsetsRequestTopicPartition()
		p.Partition, p.Timestamp = int32(i), -1
		topic.Partitions = append(topic.Partitions, p)
	}
	request.Topics = []kmsg.ListOffsetsRequestTopic{topic}
	resp, err := request.RequestWith(ctx, f.reader)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != len(want) {
		t.Fatal("incomplete Kafka end offsets")
	}
	for _, p := range resp.Topics[0].Partitions {
		if p.ErrorCode != 0 || p.Partition < 0 || int(p.Partition) >= len(want) || p.Offset != want[p.Partition] {
			t.Fatalf("unexpected end at partition %d: offset=%d err=%d", p.Partition, p.Offset, p.ErrorCode)
		}
	}
}

func assertLastVoteOffset(t *testing.T, ctx context.Context, f *voteFixture, closeOffset int64, late bool) {
	t.Helper()
	f.reader.AddConsumeTopics(f.poll.Topic)
	for {
		batch := f.reader.PollFetches(ctx)
		if errs := batch.Errors(); len(errs) > 0 {
			t.Fatal(errs)
		}
		for _, r := range batch.Records() {
			if r.Partition != 0 || r.Key == nil {
				continue
			}
			var ev struct {
				Choices []int `json:"choices"`
			}
			if err := json.Unmarshal(r.Value, &ev); err != nil {
				t.Fatal(err)
			}
			if len(ev.Choices) == 1 && ev.Choices[0] == 2 {
				if r.Offset == closeOffset || (r.Offset > closeOffset) != late {
					t.Fatalf("B vote at offset %d, first CLOSE at %d; late=%v", r.Offset, closeOffset, late)
				}
				return
			}
		}
	}
}
