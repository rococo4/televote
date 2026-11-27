//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
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
		{"missing_writer_and_late_delivery", "timeout", http.StatusInternalServerError, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			// Первая реплика сохраняет срок ожидания для этого опроса в PostgreSQL.
			// Вторая должна взять этот же срок, хотя в её настройках указано другое значение.
			const pollGrace = 2 * time.Second
			f := newPollFixture(t, ctx, 3*time.Second, pollGrace, 1, []string{"A", "B"})
			p := f.poll
			writer, err := kafka.New(f.brokers)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(writer.Close)
			gate := &delayedJournal{Producer: writer, ctx: ctx, admitted: make(chan struct{}), release: make(chan struct{}), delivered: make(chan struct{})}
			second, err := app.New(ctx, f.pool, gate, 12*time.Second, app.AdminConfig{Partitions: 4})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(second)
			t.Cleanup(server.Close)

			// Один зритель меняет ответ с A на B через другую реплику. В итог попадёт B,
			// только если запись дойдёт до партиции 0 раньше первого CLOSE.
			token := tokenForPartition(p, 0)
			path := "/api/polls/" + p.ID.String() + "/votes"
			postVote(t, ctx, f.server.Client(), f.server.URL+path, token, 1)
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+path,
				strings.NewReader(`{"choices":[2]}`))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.AddCookie(&http.Cookie{Name: voterCookie, Value: token})
			status := make(chan int, 1)
			go func() {
				res, err := server.Client().Do(req)
				if err != nil {
					status <- 0
					return
				}
				if res.StatusCode == http.StatusInternalServerError {
					var reply struct{ Code, Message string }
					if err := json.NewDecoder(res.Body).Decode(&reply); err != nil || reply.Code != "internal_error" || strings.TrimSpace(reply.Message) == "" {
						t.Errorf("unexpected HTTP 500 body: %+v, decode error: %v", reply, err)
					}
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

			startPollWorker(t, ctx, second)
			if tc.reason == "drained" {
				// Первая реплика закончила отправку. Вторая приняла голос в срок и ещё ждёт доставки в Kafka.
				startPollWorker(t, ctx, f.application)
				waitPG(ctx, t, f, `SELECT count(*) = 1 FROM poll_writers
					WHERE poll_id = $1 AND drained_at IS NOT NULL`)
				assertPartitionEnds(t, ctx, f, []int64{1, 0, 0, 0})
				assertNoFinal(t, ctx, f)
				close(gate.release)
			}
			// В сценарии timeout первая реплика не выставит drained_at. Вторая должна закрыть опрос
			// по истечении срока ожидания, даже если её собственная отправка ещё не завершилась.
			waitForFinal(t, ctx, f)
			var reason string
			var decidedAt, closesAt time.Time
			var graceMicros int64
			if err := f.pool.QueryRow(ctx, `SELECT decision_reason, decided_at, closes_at, grace_us
				FROM poll_closures WHERE poll_id=$1`, p.ID).Scan(&reason, &decidedAt, &closesAt, &graceMicros); err != nil {
				t.Fatal(err)
			}
			if graceMicros != pollGrace.Microseconds() {
				t.Fatalf("stored grace is %d µs; first writer configured %s", graceMicros, pollGrace)
			}
			delay := decidedAt.Sub(closesAt) // Обе метки взяты из PostgreSQL, сверять часы реплик не требуется.
			if reason != tc.reason || delay < 0 || (delay >= pollGrace) != (tc.reason == "timeout") {
				t.Fatalf("decision=%s delay=%s; want %s", reason, delay, tc.reason)
			}
			if tc.reason == "timeout" && delay >= pollGrace+2*time.Second {
				t.Fatalf("timeout took %s; expected %s grace plus scheduler overhead", delay, pollGrace)
			}
			select {
			case code := <-status:
				if code != tc.status {
					t.Fatalf("second HTTP status %d, want %d", code, tc.status)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if tc.reason == "timeout" {
				// Разрешаем запись в Kafka только после HTTP 500 и публикации итога.
				// Так запоздавший голос гарантированно окажется после первого CLOSE.
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

			var ballots, a, b, end int64
			err = f.pool.QueryRow(ctx, `SELECT r.ballots,(r.counts->>'1')::bigint,(r.counts->>'2')::bigint,pr.end_offset
				FROM poll_results r JOIN poll_partition_results pr USING(poll_id)
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

// Задерживаем отправку в настоящую Kafka. Запрос может завершиться по тайм-ауту,
// пока голос ещё в пути. Через WaitVotes тест дожидается завершения отправки.
type delayedJournal struct {
	*kafka.Producer
	ctx                          context.Context
	admitted, release, delivered chan struct{}
	err                          error // Читаем после закрытия delivered.
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
