//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/rococo4/televote/internal/adapter/kafka"
	"github.com/rococo4/televote/internal/app"
)

const (
	partitions  = 4
	pollVersion = 7
)

func TestSubmitVoteWritesToKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, setting("DATABASE_URL", "postgres://televote:televote@127.0.0.1:55432/televote?sslmode=disable"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	id := uuid.New()
	topic := "televote-it-" + id.String()
	brokers := strings.Split(setting("KAFKA_BROKERS", "127.0.0.1:59092"), ",")
	reader, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reader.Close)
	createTopic(t, ctx, reader, topic)
	insertOpenPoll(t, ctx, pool, id, topic)

	writer, err := kafka.New(brokers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(writer.Close)
	handler, err := app.NewHandler(ctx, pool, writer)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	// Pick a nonzero partition so a producer which always writes to 0 fails every run.
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

	// Start reading after both HTTP responses to verify the stored records.
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
			Version    int64           `json:"version"`
			ReceivedAt time.Time       `json:"received_at"`
			Choices    []int           `json:"choices"`
			Token      json.RawMessage `json:"token"`
		}
		if err := json.Unmarshal(record.Value, &event); err != nil {
			t.Fatal(err)
		}
		if event.PollID != id || event.Version != pollVersion || !slices.Equal(event.Choices, []int{choices[i]}) {
			t.Fatalf("vote %d: got poll %s version %d choices %v", i, event.PollID, event.Version, event.Choices)
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

func insertOpenPoll(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, topic string) {
	t.Helper()
	// Keep the poll and its options visible as one fixture to the startup query.
	_, err := pool.Exec(ctx, `
		WITH added AS (
			INSERT INTO polls (id, version, question, opens_at, closes_at, min_choices, max_choices, topic, partitions, published)
			VALUES ($1, $2, 'A or B?', now() - interval '1 hour', now() + interval '1 hour', 1, 1, $3, $4, true)
			RETURNING id
		)
		INSERT INTO poll_choices (poll_id, id, label)
		SELECT added.id, choice.id, choice.label FROM added
		CROSS JOIN (VALUES (1, 'A'), (2, 'B')) AS choice(id, label)`, id, pollVersion, topic, partitions)
	if err != nil {
		t.Fatalf("insert poll into migrated PostgreSQL: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := pool.Exec(ctx, "DELETE FROM polls WHERE id = $1", id); err != nil {
			t.Errorf("remove test poll: %v", err)
		}
	})
}

func createTopic(t *testing.T, ctx context.Context, client *kgo.Client, topic string) {
	t.Helper()
	create := kmsg.NewPtrCreateTopicsRequest()
	create.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1}}
	created, err := create.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}
	if len(created.Topics) != 1 || created.Topics[0].ErrorCode != 0 {
		t.Fatalf("create Kafka topic: %+v", created.Topics)
	}
	t.Cleanup(func() {
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
		} else if err := kerr.ErrorForCode(deleted.Topics[0].ErrorCode); err != nil {
			t.Errorf("delete Kafka topic: %v", err)
		}
	})
}

func postVote(t *testing.T, ctx context.Context, client *http.Client, path, token string, choice int) {
	t.Helper()
	body := fmt.Sprintf(`{"token":"%s","choices":[%d]}`, token, choice)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var reply map[string]string
	if err := json.NewDecoder(res.Body).Decode(&reply); err != nil {
		t.Fatalf("decode HTTP %d response: %v", res.StatusCode, err)
	}
	if res.StatusCode != http.StatusAccepted || reply["status"] != "accepted" {
		t.Fatalf("POST returned HTTP %d, %v", res.StatusCode, reply)
	}
}

func setting(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
