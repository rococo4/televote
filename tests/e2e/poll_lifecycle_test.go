//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/cookiejar"
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
	start := replicaStarter(t)
	a, b := start(3*time.Second), start(3*time.Second)
	now := time.Now().UTC().Truncate(time.Microsecond)
	p := pollInput{
		ID: uuid.NewString(), Question: "Which programme?", Choices: []string{"A", "B"},
		OpensAt: now.Add(-time.Minute), ClosesAt: now.Add(4 * time.Second), MinChoices: 1, MaxChoices: 1,
	}
	createURL := a.URL + "/admin/polls"
	request(t, http.DefaultClient, http.MethodPost, createURL, "", p, http.StatusUnauthorized)
	invalid := p
	invalid.MaxChoices = 3
	invalidReply := request(t, http.DefaultClient, http.MethodPost, createURL, adminToken, invalid, http.StatusBadRequest)
	checkBadRequest(t, invalidReply, "Некорректные параметры опроса.")
	id := createPoll(t, createURL, p)
	if id != p.ID {
		t.Fatalf("created poll %q, want %q", id, p.ID)
	}

	pollPath := "/api/polls/" + id
	vote := func(browser *http.Client, server *httptest.Server, choice, wantStatus int) []byte {
		t.Helper()
		return request(t, browser, http.MethodPost, server.URL+pollPath+"/votes", "",
			map[string]any{"choices": []int{choice}}, wantStatus)
	}
	openPoll := func(browser *http.Client, server *httptest.Server) {
		t.Helper()
		data := request(t, browser, http.MethodGet, server.URL+pollPath, "", nil, http.StatusOK)
		var got report
		if err := json.Unmarshal(data, &got); err != nil || got.ID != id || got.Question != p.Question ||
			len(got.Choices) != 2 || got.Choices[0].ID != 1 || got.Choices[0].Label != "A" ||
			got.Choices[1].ID != 2 || got.Choices[1].Label != "B" {
			t.Fatalf("public poll: %s (error: %v)", data, err)
		}
	}

	x, y := newBrowser(t), newBrowser(t)
	checkBadRequest(t, vote(x, a.Server, 1, http.StatusBadRequest), "Для голосования откройте опрос и разрешите cookie.")
	openPoll(x, a.Server)
	// Первая реплика выдала cookie. Вторая принимает первый голос даже с холодным кэшем.
	// Когда ответ 202 теряется, браузер повторяет выбор с той же cookie.
	lossy := &http.Client{Jar: x.Jar, Transport: loseAcceptedResponse{}, Timeout: 5 * time.Second}
	res, err := lossy.Post(b.URL+pollPath+"/votes", "application/json", strings.NewReader(`{"choices":[1]}`))
	if err == nil {
		res.Body.Close()
		t.Fatalf("expected a lost HTTP 202, got HTTP %d", res.StatusCode)
	}
	if !errors.Is(err, errVoteResponseLost) {
		t.Fatal(err)
	}
	openPoll(x, b.Server)
	vote(x, a.Server, 1, http.StatusAccepted)
	vote(x, b.Server, 2, http.StatusAccepted)
	openPoll(y, b.Server)
	vote(y, a.Server, 2, http.StatusAccepted)

	if retry := createPoll(t, b.URL+"/admin/polls", p); retry != id {
		t.Fatalf("same create request returned %q instead of %q", retry, id)
	}
	conflict := p
	conflict.Question = "A different question"
	conflictReply := request(t, http.DefaultClient, http.MethodPost, createURL, adminToken, conflict, http.StatusBadRequest)
	checkBadRequest(t, conflictReply, "Этот ID занят опросом с другими параметрами.")
	path := "/admin/polls/" + id + "/results"
	request(t, http.DefaultClient, http.MethodGet, a.URL+path, "incorrect", nil, http.StatusUnauthorized)
	open := readReport(t, a.URL+path)
	if open.Status != "open" {
		t.Fatalf("status during voting = %q", open.Status)
	}
	final := awaitFinal(t, b.URL+path)
	if final.ID != id || final.Question != p.Question || final.Ballots == nil || *final.Ballots != 2 {
		t.Fatalf("unexpected final report: %+v", final)
	}
	assertChoices(t, final, []int{0, 2})
	checkBadRequest(t, vote(x, a.Server, 1, http.StatusBadRequest), "Опрос закрыт.")
}

func checkBadRequest(t *testing.T, body []byte, message string) {
	t.Helper()
	var reply struct{ Code, Message string }
	if err := json.Unmarshal(body, &reply); err != nil || reply.Code != "bad_request" || reply.Message != message {
		t.Fatalf("expected bad_request with message %q, got %s", message, body)
	}
}

func newBrowser(t *testing.T) *http.Client {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	return &http.Client{Jar: jar}
}

type loseAcceptedResponse struct{}

var errVoteResponseLost = errors.New("discarded HTTP 202")

func (loseAcceptedResponse) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := http.DefaultTransport.RoundTrip(req)
	if err != nil || res.StatusCode != http.StatusAccepted {
		return res, err
	}
	res.Body.Close()
	return nil, errVoteResponseLost
}

func TestSeparateBroadcastsOfSameQuestion(t *testing.T) {
	start := replicaStarter(t)
	a, b := start(3*time.Second), start(3*time.Second)
	now := time.Now().UTC().Truncate(time.Microsecond)
	p := pollInput{
		Question: "A или B?", Choices: []string{"A", "B"},
		OpensAt: now.Add(-time.Second), ClosesAt: now.Add(4 * time.Second), MinChoices: 1, MaxChoices: 1,
	}
	// Одинаковый вопрос можно задать в двух эфирах. Каждый POST без ID создаёт новый опрос.
	first := createPoll(t, a.URL+"/admin/polls", p)
	second := createPoll(t, b.URL+"/admin/polls", p)
	if first == second {
		t.Fatal("separate broadcasts received the same poll ID")
	}
	browser := newBrowser(t)
	for i, id := range []string{first, second} {
		pollURL := b.URL + "/api/polls/" + id
		request(t, browser, http.MethodGet, pollURL, "", nil, http.StatusOK)
		request(t, browser, http.MethodPost, pollURL+"/votes", "", map[string]any{"choices": []int{i + 1}}, http.StatusAccepted)
	}
	for i, id := range []string{first, second} {
		final := awaitFinal(t, a.URL+"/admin/polls/"+id+"/results")
		if final.ID != id || final.Ballots == nil || *final.Ballots != 1 {
			t.Fatalf("unexpected final report for broadcast %s: %+v", id, final)
		}
		want := []int{0, 0}
		want[i] = 1
		assertChoices(t, final, want)
	}
}

func TestEmptyPollSurvivesCreatorShutdown(t *testing.T) {
	start := replicaStarter(t)
	a := start(3 * time.Second)
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
	// Создатель опроса остановился до drained_at. Б запустится уже после эфира,
	// возьмёт оставшееся время из PostgreSQL и опубликует итог без новых голосов.
	time.Sleep(time.Until(p.ClosesAt.Add(50 * time.Millisecond)))
	b := start(time.Minute)
	if interim := readReport(t, b.URL+path); interim.Status != "finalizing" {
		t.Fatalf("status after deadline while a writer is missing = %q", interim.Status)
	}
	final := awaitFinal(t, b.URL+path)
	if final.ID != id || final.Ballots == nil || *final.Ballots != 0 {
		t.Fatalf("unexpected empty result: %+v", final)
	}
	assertChoices(t, final, []int{0, 0})
}

func request(t *testing.T, client *http.Client, method, url, auth string, input any, want int) []byte {
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
	res, err := client.Do(req)
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
	data := request(t, http.DefaultClient, http.MethodPost, url, adminToken, p, http.StatusCreated)
	var created struct{ ID string }
	if err := json.Unmarshal(data, &created); err != nil || uuid.Validate(created.ID) != nil {
		t.Fatalf("expected created poll ID, got %s", data)
	}
	return created.ID
}

func readReport(t *testing.T, url string) report {
	t.Helper()
	data := request(t, http.DefaultClient, http.MethodGet, url, adminToken, nil, http.StatusOK)
	var r report
	// Проверяем состав ответа, в том числе поля каждого варианта. Данные отдельных бюллетеней в отчёт не входят.
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&r); err != nil {
		t.Fatalf("invalid report: %v; body=%s", err, data)
	}
	if r.Status != "final" {
		// RawMessage отличает отсутствие поля от явного null.
		var totals struct {
			Ballots json.RawMessage
			Choices []struct{ Votes, Percent json.RawMessage }
		}
		if err := json.Unmarshal(data, &totals); err != nil {
			t.Fatal(err)
		}
		if totals.Ballots != nil {
			t.Fatalf("partial ballot count exposed: %s", data)
		}
		for _, c := range totals.Choices {
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

func replicaStarter(t *testing.T) func(time.Duration) replica {
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

	return func(grace time.Duration) replica {
		pool, err := pgxpool.NewWithConfig(t.Context(), cfg.Copy())
		if err != nil {
			t.Fatal(err)
		}
		journal, err := kafka.New(brokers)
		if err != nil {
			pool.Close()
			t.Fatal(err)
		}
		application, err := app.New(t.Context(), pool, journal, grace, app.AdminConfig{Token: adminToken, Partitions: 4})
		if err != nil {
			journal.Close()
			pool.Close()
			t.Fatal(err)
		}
		server := httptest.NewServer(application)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			defer close(done)
			application.ServePolls(ctx)
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
