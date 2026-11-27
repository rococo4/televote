//go:build integration && load

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type voteSample struct {
	status   int
	latency  time.Duration
	finished time.Time
	err      error
}

func TestSubmitVoteLoad(t *testing.T) {
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// После минуты нагрузки оставляем десять секунд запаса на задержки локального генератора.
	f := newVoteFixture(t, ctx, 70*time.Second)
	startPollWorker(t, ctx, f.application)
	id, topic, reader, server := f.poll.ID, f.poll.Topic, f.reader, f.server
	path := server.URL + "/api/polls/" + id.String() + "/votes"
	const maxInflight = 128
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.MaxIdleConns = maxInflight
	transport.MaxIdleConnsPerHost = maxInflight
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 8 * time.Second}

	const perPhase = 15000
	const total = 2 * perPhase
	bodies := make([]string, total)
	// Измеряем приём голосов со знакомыми cookie. Выдача cookie при открытии опроса в этот замер не входит.
	cookies := make([]string, total)
	keys := make([]string, total)
	wantCounts := make(map[int]int64)
	for i := range bodies {
		// На каждые десять зрителей приходится одиннадцать запросов. Десятый зритель отправляет свой ответ дважды.
		voter := i/11*10 + min(i%11, 9)
		if i%11 != 10 {
			wantCounts[voter%2+1]++
		}
		var token [16]byte
		binary.BigEndian.PutUint64(token[8:], uint64(voter+1))
		cookies[i] = hex.EncodeToString(token[:])
		bodies[i] = fmt.Sprintf(`{"choices":[%d]}`, voter%2+1)
		var identity [32]byte
		copy(identity[:16], id[:])
		copy(identity[16:], token[:])
		key := sha256.Sum256(identity[:])
		keys[i] = string(key[:])
	}

	samples := make([]voteSample, total)
	var pending sync.WaitGroup
	slots := make(chan struct{}, maxInflight)
	durations := []time.Duration{15 * time.Second, 45 * time.Second}
	phaseStarts := make([]time.Time, len(durations))
	for phase, duration := range durations {
		phaseStarts[phase] = time.Now()
		next := phaseStarts[phase]
		interval := duration / perPhase
		for n := range perPhase {
			time.Sleep(time.Until(next))
			// После паузы планировщика сохраняем интервал между запросами, иначе генератор пошлёт их залпом.
			if now := time.Now(); now.Sub(next) >= interval {
				next = now
			}
			next = next.Add(interval)
			i := phase*perPhase + n
			select {
			case slots <- struct{}{}:
				pending.Go(func() {
					defer func() { <-slots }()
					samples[i] = measureVote(ctx, client, path, bodies[i], cookies[i])
				})
			default:
				// Генератор исчерпал лимит одновременных запросов. В отчёте эта попытка попадёт в dropped.
			}
		}
		time.Sleep(time.Until(next))
	}
	pending.Wait()
	waitForFinal(t, ctx, f)
	finalObservedAt := time.Now()
	for phase, duration := range durations {
		reportVotePhase(t, samples[phase*perPhase:(phase+1)*perPhase], phaseStarts[phase], duration)
	}
	var ballots int64
	var counts map[int]int64
	err := f.pool.QueryRow(ctx, `SELECT ballots, counts FROM poll_results
		WHERE poll_id = $1`, id).Scan(&ballots, &counts)
	if err != nil {
		t.Fatal(err)
	}
	if ballots != wantCounts[1]+wantCounts[2] || !maps.Equal(counts, wantCounts) {
		t.Errorf("final: %d ballots, counts %v; expected %v", ballots, counts, wantCounts)
	}
	finalDelay := finalObservedAt.Sub(f.poll.ClosesAt)
	if finalDelay < 0 || finalDelay > time.Since(started) {
		t.Fatalf("inconsistent finalization interval %s", finalDelay)
	}
	t.Logf("PostgreSQL: %d ballots, A=%d B=%d, observed final %s after poll deadline", ballots, counts[1], counts[2], finalDelay)
	readCtx, stop := context.WithTimeout(ctx, 20*time.Second)
	defer stop()
	gotKeys := readLoadVoteKeys(t, readCtx, reader, topic)
	// Даже если клиент не дождался ответа, запись могла попасть в Kafka. Для каждого ключа
	// число записей должно быть не меньше числа ответов 202 и не больше числа отправленных запросов.
	attempts, confirmed := make(map[string]int), make(map[string]int)
	for i, sample := range samples {
		if !sample.finished.IsZero() {
			attempts[keys[i]]++
		}
		if sample.status == http.StatusAccepted && sample.err == nil {
			confirmed[keys[i]]++
		}
	}
	for key, n := range confirmed {
		if gotKeys[key] < n {
			t.Fatalf("key %x has %d Kafka records for %d confirmed votes", key, gotKeys[key], n)
		}
	}
	for key, n := range gotKeys {
		if n > attempts[key] {
			t.Fatalf("key %x has %d Kafka records for %d attempted votes", key, n, attempts[key])
		}
	}
}

func measureVote(ctx context.Context, client *http.Client, path, body, cookie string) (s voteSample) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, path, strings.NewReader(body))
	if err != nil {
		panic(err) // Метод и URL задаёт сам тест, поэтому ошибка означает сбой генератора.
	}
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: voterCookie, Value: cookie})
	start := time.Now()
	defer func() {
		s.finished = time.Now()
		s.latency = s.finished.Sub(start)
	}()
	res, err := client.Do(req)
	if err != nil {
		s.err = err
		return
	}
	defer res.Body.Close()
	s.status = res.StatusCode
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		s.err = err
		return
	}
	if s.status == http.StatusAccepted {
		var reply struct{ Status string }
		if json.Unmarshal(bodyBytes, &reply) != nil || reply.Status != "accepted" {
			s.err = fmt.Errorf("invalid HTTP 202 body %q", bodyBytes)
		}
	}
	return
}

func reportVotePhase(t *testing.T, samples []voteSample, start time.Time, duration time.Duration) {
	t.Helper()
	status := make(map[int]int)
	var latencies []time.Duration
	var accepted, failures int
	var firstError error
	end := start.Add(duration)
	for _, sample := range samples {
		if sample.finished.IsZero() {
			continue
		}
		if sample.finished.After(end) {
			end = sample.finished
		}
		latencies = append(latencies, sample.latency)
		status[sample.status]++
		if sample.err != nil {
			failures++
			if firstError == nil {
				firstError = sample.err
			}
		} else if sample.status == http.StatusAccepted {
			accepted++
		}
	}
	slices.Sort(latencies)
	percentile := func(p int) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[(len(latencies)*p+99)/100-1]
	}
	elapsed := end.Sub(start)
	t.Logf("phase=%s offered=%d attempted=%d dropped=%d statuses=%v transport_or_response_errors=%d accepted=%d elapsed=%s attempted/s=%.1f accepted/s=%.1f p95=%s p99=%s",
		duration, len(samples), len(latencies), len(samples)-len(latencies), status, failures, accepted, elapsed, float64(len(latencies))/elapsed.Seconds(), float64(accepted)/elapsed.Seconds(), percentile(95), percentile(99))
	if accepted != len(samples) {
		t.Errorf("phase %s: accepted %d of %d offered, first transport/response error: %v", duration, accepted, len(samples), firstError)
	}
}

func readLoadVoteKeys(t *testing.T, ctx context.Context, reader *kgo.Client, topic string) map[string]int {
	t.Helper()
	request := kmsg.NewPtrListOffsetsRequest()
	partitionQuery := kmsg.ListOffsetsRequestTopic{Topic: topic}
	for i := range partitions {
		p := kmsg.NewListOffsetsRequestTopicPartition()
		p.Partition = int32(i)
		// Запрашиваем следующую позицию после последней записи. В тестовом топике без транзакций
		// и удалённых записей она совпадает с числом записей в партиции.
		p.Timestamp = -1
		partitionQuery.Partitions = append(partitionQuery.Partitions, p)
	}
	request.Topics = []kmsg.ListOffsetsRequestTopic{partitionQuery}
	response, err := request.RequestWith(ctx, reader)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Topics) != 1 || response.Topics[0].Topic != topic || len(response.Topics[0].Partitions) != partitions {
		t.Fatalf("unexpected end-offset response: %+v", response.Topics)
	}
	var total int64
	for _, p := range response.Topics[0].Partitions {
		if p.ErrorCode != 0 || p.Offset < 0 {
			t.Fatalf("read end offset: %+v", p)
		}
		total += p.Offset
	}
	reader.AddConsumeTopics(topic)
	keys := make(map[string]int)
	var count int64
	var markers int64
	for count < total {
		batch := reader.PollFetches(ctx)
		if errs := batch.Errors(); len(errs) != 0 {
			t.Fatalf("consume load records: %v", errs)
		}
		batch.EachRecord(func(r *kgo.Record) {
			count++
			if r.Key == nil {
				var event struct{ Type string }
				if json.Unmarshal(r.Value, &event) != nil || event.Type != "CLOSE" {
					t.Fatalf("unexpected unkeyed event at %d/%d", r.Partition, r.Offset)
				}
				markers++
				return
			}
			keys[string(r.Key)]++
		})
	}
	t.Logf("Kafka: %d votes and %d close markers across %d partitions, %d distinct voter keys", count-markers, markers, partitions, len(keys))
	return keys
}
