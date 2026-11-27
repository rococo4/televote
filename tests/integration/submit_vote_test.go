//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
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
	"github.com/rococo4/televote/internal/entity"
)

const (
	partitions     = 4
	defaultDB      = "postgres://televote:televote@127.0.0.1:55432/televote?sslmode=disable"
	testCloseGrace = 5 * time.Second
	voterCookie    = "televote_voter"
)

func TestSubmitVoteWritesToKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	f := newVoteFixture(t, ctx, time.Hour)
	id, topic, reader, server := f.poll.ID, f.poll.Topic, f.reader, f.server

	// Подбираем ключ для ненулевой партиции, чтобы заметить ошибку, при которой все голоса уходят в партицию 0.
	var token uuid.UUID
	var key [sha256.Size]byte
	var wantPartition int32
	for wantPartition == 0 {
		token = uuid.New()
		var identity [32]byte
		copy(identity[:16], id[:])
		copy(identity[16:], token[:])
		key = sha256.Sum256(identity[:])
		wantPartition = int32(binary.BigEndian.Uint64(key[:8]) % partitions)
	}
	tokenHex := hex.EncodeToString(token[:])
	path := server.URL + "/api/polls/" + id.String() + "/votes"
	choices := []int{1, 2}
	type requestWindow struct{ start, end time.Time }
	windows := make([]requestWindow, len(choices))
	for i, choice := range choices {
		windows[i].start = time.Now()
		postVote(t, ctx, server.Client(), path, tokenHex, choice)
		windows[i].end = time.Now()
	}

	// После двух ответов HTTP 202 обе записи должны быть доступны независимому читателю Kafka.
	reader.AddConsumeTopics(topic)
	var records []*kgo.Record
	for len(records) < len(choices) {
		batch := reader.PollFetches(ctx)
		if errs := batch.Errors(); len(errs) != 0 {
			t.Fatalf("read votes from Kafka: %v", errs)
		}
		records = append(records, batch.Records()...)
	}
	if len(records) != len(choices) {
		t.Fatalf("read %d records for %d POSTs", len(records), len(choices))
	}
	for i, record := range records {
		if record.Topic != topic || record.Partition != wantPartition || !bytes.Equal(record.Key, key[:]) {
			t.Fatalf("vote %d routed to %s[%d] key %x, want %s[%d] key %x", i, record.Topic, record.Partition, record.Key, topic, wantPartition, key)
		}
		if i > 0 && record.Offset <= records[i-1].Offset {
			t.Fatalf("offsets out of order: %d then %d", records[i-1].Offset, record.Offset)
		}
		var event struct {
			PollID     uuid.UUID       `json:"poll_id"`
			ReceivedAt time.Time       `json:"received_at"`
			Choices    []int           `json:"choices"`
			Token      json.RawMessage `json:"token"`
		}
		if err := json.Unmarshal(record.Value, &event); err != nil {
			t.Fatal(err)
		}
		if event.PollID != id || !slices.Equal(event.Choices, []int{choices[i]}) {
			t.Fatalf("vote %d: got poll %s choices %v", i, event.PollID, event.Choices)
		}
		if event.ReceivedAt.Before(windows[i].start) || event.ReceivedAt.After(windows[i].end) {
			t.Fatalf("vote %d received_at %s outside its HTTP request window %v", i, event.ReceivedAt, windows[i])
		}
		if len(event.Token) != 0 || bytes.Contains(record.Value, []byte(tokenHex)) {
			t.Fatal("Kafka value contains the browser token")
		}
	}
	t.Logf("2 HTTP 202 responses; consumed 2 votes in %s[%d], offsets %d and %d", topic, wantPartition, records[0].Offset, records[1].Offset)
}

type voteFixture struct {
	poll        entity.Poll
	reader      *kgo.Client
	server      *httptest.Server
	pool        *pgxpool.Pool
	application *app.App
	writer      *kafka.Producer
	brokers     []string
}

func newVoteFixture(t *testing.T, ctx context.Context, openFor time.Duration) *voteFixture {
	t.Helper()
	return newPollFixture(t, ctx, openFor, testCloseGrace, 1, []string{"A", "B"})
}

func newPollFixture(t *testing.T, ctx context.Context, openFor, closeGrace time.Duration, maxChoices int, labels []string) *voteFixture {
	t.Helper()
	pool := newTestDatabase(t, ctx)
	id := uuid.New()
	topic := "televote-it-" + id.String()
	brokers := strings.Split(setting("KAFKA_BROKERS", "127.0.0.1:59092"), ",")
	reader, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reader.Close)
	writer, err := kafka.New(brokers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(writer.Close)
	poll := entity.Poll{
		ID: id, MinChoices: 1, MaxChoices: maxChoices, Choices: make(map[int]struct{}, len(labels)),
		Topic: topic, Partitions: partitions,
	}
	for i := range labels {
		poll.Choices[i+1] = struct{}{}
	}
	t.Cleanup(func() { deleteTopic(t, reader, topic) })
	if _, err := writer.EnsureTopic(ctx, poll); err != nil {
		t.Fatalf("prepare Kafka topic: %v", err)
	}
	// Отсчитываем окно голосования, когда топик готов. Сохраняем монотонное время
	// для замера задержки и округляем до точности PostgreSQL.
	now := time.Now()
	now = now.Add(-time.Duration(now.Nanosecond() % 1000))
	poll.OpensAt, poll.ClosesAt = now.Add(-time.Hour), now.Add(openFor)
	insertOpenPoll(t, ctx, pool, poll, labels)

	application, err := app.New(ctx, pool, writer, closeGrace, app.AdminConfig{Partitions: 4})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(application)
	t.Cleanup(server.Close)

	return &voteFixture{
		poll: poll, reader: reader, server: server, pool: pool, application: application,
		writer: writer, brokers: brokers,
	}
}

func insertOpenPoll(t *testing.T, ctx context.Context, pool *pgxpool.Pool, poll entity.Poll, labels []string) {
	t.Helper()
	// Одним SQL-запросом создаём опрос вместе с вариантами. При запуске сервис увидит его целиком.
	_, err := pool.Exec(ctx, `
		WITH added AS (
			INSERT INTO polls (id, question, opens_at, closes_at, min_choices, max_choices, topic, partitions, published)
			VALUES ($1, 'Choose an answer', $2, $3, 1, $4, $5, $6, true)
			RETURNING id
		)
		INSERT INTO poll_choices (poll_id, id, label)
		SELECT added.id, choice.id, choice.label FROM added
		CROSS JOIN unnest($7::text[]) WITH ORDINALITY AS choice(label, id)`,
		poll.ID, poll.OpensAt, poll.ClosesAt, poll.MaxChoices, poll.Topic, poll.Partitions, labels)
	if err != nil {
		t.Fatalf("insert poll into migrated PostgreSQL: %v", err)
	}
}

func deleteTopic(t *testing.T, client *kgo.Client, topic string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	del := kmsg.NewPtrDeleteTopicsRequest()
	del.Topics = []kmsg.DeleteTopicsRequestTopic{{Topic: &topic}}
	del.TopicNames = []string{topic}
	deleted, err := del.RequestWith(ctx, client)
	if err != nil {
		t.Errorf("delete Kafka topic: %v", err)
	} else if len(deleted.Topics) != 1 {
		t.Errorf("unexpected delete response: %+v", deleted.Topics)
	} else if err := kerr.ErrorForCode(deleted.Topics[0].ErrorCode); err != nil && err != kerr.UnknownTopicOrPartition {
		t.Errorf("delete Kafka topic: %v", err)
	}
}

func postVote(t *testing.T, ctx context.Context, client *http.Client, path, token string, choices ...int) {
	t.Helper()
	status, reply := sendBallot(t, ctx, client, path, token, choices)
	if status != http.StatusAccepted || reply["status"] != "accepted" {
		t.Fatalf("POST returned HTTP %d, %v", status, reply)
	}
}

func sendBallot(t *testing.T, ctx context.Context, client *http.Client, path, token string, choices []int) (int, map[string]string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"choices": choices})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: voterCookie, Value: token})
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var reply map[string]string
	if err := json.NewDecoder(res.Body).Decode(&reply); err != nil {
		t.Fatalf("decode HTTP %d response: %v", res.StatusCode, err)
	}
	return res.StatusCode, reply
}

func setting(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func newTestDatabase(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	// При запуске сервис загружает все опубликованные опросы. Отдельная схема оставляет в выборке только данные теста.
	cfg, err := pgxpool.ParseConfig(setting("DATABASE_URL", defaultDB))
	if err != nil {
		t.Fatal(err)
	}
	schema := pgx.Identifier{"televote_count_" + uuid.NewString()}.Sanitize()
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := pool.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("remove isolated poll schema: %v", err)
		}
	})
	files, err := filepath.Glob("../../internal/migrations/postgres/*.sql")
	if err != nil || len(files) == 0 {
		t.Fatalf("find migrations: %v (%v)", err, files)
	}
	for _, path := range files {
		sql, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", path, err)
		}
	}
	return pool
}
