//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/rococo4/televote/internal/adapter/kafka"
	"github.com/rococo4/televote/internal/app"
)

const adminToken = "local-test-editor"

type pollInput struct {
	ID         string    `json:"id,omitempty"`
	Question   string    `json:"question"`
	Choices    []string  `json:"choices"`
	OpensAt    time.Time `json:"opens_at"`
	ClosesAt   time.Time `json:"closes_at"`
	MinChoices int       `json:"min_choices"`
	MaxChoices int       `json:"max_choices"`
}

type report struct {
	ID       string `json:"id"`
	Question string `json:"question"`
	Status   string `json:"status"`
	Ballots  *int   `json:"ballots"`
	Choices  []struct {
		ID      int             `json:"id"`
		Label   string          `json:"label"`
		Votes   *int            `json:"votes"`
		Percent json.RawMessage `json:"percent"`
	} `json:"choices"`
}

func TestPollLifecycle(t *testing.T) {
	a, b := startReplicas(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	p := pollInput{
		ID: uuid.NewString(), Question: "Which programme?", Choices: []string{"A", "B"},
		OpensAt: now.Add(-time.Minute), ClosesAt: now.Add(4 * time.Second), MinChoices: 1, MaxChoices: 1,
	}
	createURL := a.URL + "/admin/polls"
	request(t, http.MethodPost, createURL, "", p, http.StatusUnauthorized)
	invalid := p
	invalid.MaxChoices = 3
	request(t, http.MethodPost, createURL, adminToken, invalid, http.StatusUnprocessableEntity)
	id := createPoll(t, createURL, p)
	if id != p.ID {
		t.Fatalf("created poll %q, want %q", id, p.ID)
	}

	// The second instance was already running when A created the poll.
	// Send its first vote immediately, without priming its cache or retrying.
	vote := func(server *httptest.Server, token string, choice, wantStatus int) {
		t.Helper()
		request(t, http.MethodPost, server.URL+"/api/polls/"+id+"/votes", "",
			map[string]any{"token": token, "choices": []int{choice}}, wantStatus)
	}
	const x, y = "10000000000000000000000000000001", "20000000000000000000000000000002"
	vote(b.Server, x, 1, http.StatusAccepted)
	vote(a.Server, x, 2, http.StatusAccepted)
	vote(b.Server, y, 2, http.StatusAccepted)

	if retry := createPoll(t, b.URL+"/admin/polls", p); retry != id {
		t.Fatalf("same create request returned %q instead of %q", retry, id)
	}
	conflict := p
	conflict.Question = "A different question"
	request(t, http.MethodPost, createURL, adminToken, conflict, http.StatusConflict)
	path := "/admin/polls/" + id + "/results"
	request(t, http.MethodGet, a.URL+path, "incorrect", nil, http.StatusUnauthorized)
	open := readReport(t, a.URL+path)
	if open.Status != "open" {
		t.Fatalf("status during voting = %q", open.Status)
	}
	final := awaitFinal(t, b.URL+path)
	if final.ID != id || final.Question != p.Question || final.Ballots == nil || *final.Ballots != 2 {
		t.Fatalf("unexpected final report: %+v", final)
	}
	assertChoices(t, final, []int{0, 2})
	vote(a.Server, x, 1, http.StatusGone)
}

func TestEmptyPollSurvivesCreatorShutdown(t *testing.T) {
	a, b := startReplicas(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	p := pollInput{
		Question: "Never voted on", Choices: []string{"A", "B"},
		OpensAt: now.Add(3 * time.Second), ClosesAt: now.Add(4 * time.Second), MinChoices: 1, MaxChoices: 1,
	}
	id := createPoll(t, a.URL+"/admin/polls", p)
	path := "/admin/polls/" + id + "/results"
	if scheduled := readReport(t, a.URL+path); scheduled.Status != "scheduled" {
		t.Fatalf("status before opening = %q", scheduled.Status)
	}
	a.stop()
	// The creator cannot acknowledge its drain. The grace period keeps
	// the report provisional while the remaining instance takes over.
	time.Sleep(time.Until(p.ClosesAt.Add(50 * time.Millisecond)))
	if interim := readReport(t, b.URL+path); interim.Status != "finalizing" {
		t.Fatalf("status after deadline while a writer is missing = %q", interim.Status)
	}
	final := awaitFinal(t, b.URL+path)
	if final.ID != id || final.Ballots == nil || *final.Ballots != 0 {
		t.Fatalf("unexpected empty result: %+v", final)
	}
	assertChoices(t, final, []int{0, 0})
}

func request(t *testing.T, method, url, auth string, input any, want int) []byte {
	t.Helper()
	var body bytes.Buffer
	if input != nil {
		if err := json.NewEncoder(&body).Encode(input); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, &body)
	if err != nil {
		t.Fatal(err)
	}
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil || res.StatusCode != want {
		t.Fatalf("%s %s: HTTP %d, want %d; body=%s, read error=%v", method, req.URL.Path, res.StatusCode, want, data, err)
	}
	return data
}

func createPoll(t *testing.T, url string, p pollInput) string {
	t.Helper()
	data := request(t, http.MethodPost, url, adminToken, p, http.StatusCreated)
	var created struct{ ID string }
	if err := json.Unmarshal(data, &created); err != nil || uuid.Validate(created.ID) != nil {
		t.Fatalf("expected created poll ID, got %s", data)
	}
	return created.ID
}

func readReport(t *testing.T, url string) report {
	t.Helper()
	data := request(t, http.MethodGet, url, adminToken, nil, http.StatusOK)
	var r report
	// The public result contract admits only aggregate fields, including inside choices.
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&r); err != nil {
		t.Fatalf("invalid report: %v; body=%s", err, data)
	}
	if r.Status != "final" {
		if r.Ballots != nil {
			t.Fatalf("partial ballot count exposed: %s", data)
		}
		for _, c := range r.Choices {
			if c.Votes != nil || c.Percent != nil {
				t.Fatalf("partial option totals exposed: %s", data)
			}
		}
	}
	return r
}

func awaitFinal(t *testing.T, url string) report {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		r := readReport(t, url)
		switch r.Status {
		case "final":
			return r
		case "scheduled", "open", "finalizing":
		default:
			t.Fatalf("unknown poll status %q", r.Status)
		}
		if time.Now().After(deadline) {
			t.Fatalf("automatic counting timed out; last status %q", r.Status)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func assertChoices(t *testing.T, r report, votes []int) {
	t.Helper()
	if len(r.Choices) != len(votes) {
		t.Fatalf("expected %d choices, got %+v", len(votes), r.Choices)
	}
	for i, c := range r.Choices {
		if c.ID != i+1 || c.Label != []string{"A", "B"}[i] || c.Votes == nil || *c.Votes != votes[i] {
			t.Fatalf("unexpected choice %d: %+v (expected %d votes)", i+1, c, votes[i])
		}
		if *r.Ballots == 0 {
			if string(c.Percent) != "null" {
				t.Fatalf("empty poll choice %d: percent must be null, got %s", c.ID, c.Percent)
			}
		} else {
			var percent *float64
			want := 100 * float64(votes[i]) / float64(*r.Ballots)
			if err := json.Unmarshal(c.Percent, &percent); err != nil || percent == nil || *percent != want {
				t.Fatalf("choice %d: percent=%s, want %g", c.ID, c.Percent, want)
			}
		}
	}
}

type replica struct {
	*httptest.Server
	stop func()
}

func startReplicas(t *testing.T) (replica, replica) {
	t.Helper()
	brokers := strings.Split(setting("KAFKA_BROKERS", "127.0.0.1:59092"), ",")
	cfg, err := pgxpool.ParseConfig(setting("DATABASE_URL", "postgres://televote:televote@127.0.0.1:55432/televote?sslmode=disable"))
	if err != nil {
		t.Fatal(err)
	}
	schema := pgx.Identifier{"televote_e2e_" + uuid.NewString()}.Sanitize()
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	db, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if _, err := db.Exec(t.Context(), "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := db.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("remove test schema: %v", err)
		}
	})
	files, err := filepath.Glob("../../internal/migrations/postgres/*.sql")
	if err != nil || len(files) == 0 {
		t.Fatalf("find migrations: %v, found %v", err, files)
	}
	for _, path := range files {
		sql, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(t.Context(), string(sql)); err != nil {
			t.Fatalf("apply %s: %v", path, err)
		}
	}
	t.Cleanup(func() { deleteTestTopics(t, db, brokers) })

	start := func() replica {
		pool, err := pgxpool.NewWithConfig(t.Context(), cfg.Copy())
		if err != nil {
			t.Fatal(err)
		}
		journal, err := kafka.New(brokers)
		if err != nil {
			pool.Close()
			t.Fatal(err)
		}
		h, err := app.NewHandler(t.Context(), pool, journal, 3*time.Second, app.AdminConfig{Token: adminToken, Partitions: 4})
		if err != nil {
			journal.Close()
			pool.Close()
			t.Fatal(err)
		}
		server := httptest.NewServer(h)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			defer close(done)
			h.ServePolls(ctx)
		}()
		stop := sync.OnceFunc(func() {
			server.Close()
			cancel()
			<-done
			journal.Close()
			pool.Close()
		})
		t.Cleanup(stop)
		return replica{server, stop}
	}
	return start(), start()
}

func deleteTestTopics(t *testing.T, db *pgxpool.Pool, brokers []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := db.Query(ctx, "SELECT topic FROM polls")
	if err != nil {
		t.Error(err)
		return
	}
	topics, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Error(err)
		return
	}
	if len(topics) == 0 {
		return
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Error(err)
		return
	}
	defer client.Close()
	del := kmsg.NewPtrDeleteTopicsRequest()
	del.TopicNames = topics
	for _, topic := range topics {
		del.Topics = append(del.Topics, kmsg.DeleteTopicsRequestTopic{Topic: &topic})
	}
	res, err := del.RequestWith(ctx, client)
	if err != nil {
		t.Error(err)
		return
	}
	for _, topic := range res.Topics {
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
			t.Errorf("remove test topic %v: %v", topic.Topic, err)
		}
	}
}

func setting(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
