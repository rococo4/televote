//go:build e2e && load

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTwoProcessVotingLoad(t *testing.T) {
	brokers := strings.Split(setting("KAFKA_BROKERS", "127.0.0.1:59092"), ",")
	dsn := isolatedDatabase(t, brokers)
	binary := filepath.Join(t.TempDir(), "televote")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "../../cmd/televote")
	if data, err := build.CombinedOutput(); err != nil {
		t.Fatalf("сборка: %v\n%s", err, data)
	}

	// Заголовок добавляет только тестовый балансировщик. По нему клиент узнаёт,
	// какая реплика действительно приняла каждый из двух ответов зрителя.
	var next atomic.Uint64
	var upstreams [2]*httputil.ReverseProxy
	for i := range upstreams {
		upstreams[i] = httputil.NewSingleHostReverseProxy(startProcess(t, binary, dsn, brokers))
	}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idx := (next.Add(1) - 1) % 2
		w.Header().Set("X-Test-Replica", []string{"A", "B"}[idx])
		upstreams[idx].ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)

	now := time.Now().UTC().Truncate(time.Microsecond)
	poll := pollInput{
		Question: "Какой вариант выбираете?", Choices: []string{"A", "B"},
		MinChoices: 1, MaxChoices: 1, OpensAt: now.Add(-time.Second), ClosesAt: now.Add(32 * time.Second),
	}
	id := createPoll(t, proxy.URL+"/admin/polls", poll)
	pollURL := proxy.URL + "/api/polls/" + id
	const viewersPerPhase = 1_000
	var crossing, failures int
	var totalGET, totalPOST [2]int

	for _, duration := range []time.Duration{5 * time.Second, 15 * time.Second} {
		samples := make([]viewerResult, viewersPerPhase)
		start := time.Now()
		slots := make(chan struct{}, 64)
		var jobs sync.WaitGroup
		for i := range samples {
			time.Sleep(time.Until(start.Add(time.Duration(i) * duration / viewersPerPhase)))
			slots <- struct{}{}
			jobs.Go(func() {
				defer func() { <-slots }()
				samples[i] = runViewer(t.Context(), pollURL, id, i%10 == 0)
			})
		}
		jobs.Wait()
		// Последний запрос мог успеть за долю секунды до планового конца фазы.
		elapsed := max(time.Since(start), duration)
		var gets, posts [2]int
		var latencies []time.Duration
		failed := 0
		for _, sample := range samples {
			if sample.err != nil {
				if failed == 0 {
					t.Logf("первая ошибка в фазе %s: %v", duration, sample.err)
				}
				failed++
			}
			if sample.crossed {
				crossing++
			}
			for i := range gets {
				gets[i] += sample.gets[i]
				posts[i] += sample.posts[i]
			}
			latencies = append(latencies, sample.latencies...)
		}
		for i := range gets {
			totalGET[i] += gets[i]
			totalPOST[i] += posts[i]
		}
		failures += failed
		slices.Sort(latencies)
		var p95, p99 time.Duration
		if len(latencies) > 0 {
			p95 = latencies[(95*len(latencies)-1)/100]
			p99 = latencies[(99*len(latencies)-1)/100]
		}
		t.Logf("фаза=%s зрителей=%d GET_200=%v POST_202=%v HTTP_успешных/с=%.1f POST_202/с=%.1f p95_POST=%s p99_POST=%s ошибок_сценария=%d",
			duration, len(samples), gets, posts, float64(gets[0]+gets[1]+posts[0]+posts[1])/elapsed.Seconds(),
			float64(posts[0]+posts[1])/elapsed.Seconds(), p95, p99, failed)
	}
	t.Logf("GET_200_по_репликам=%v POST_202_по_репликам=%v смен_ответа_через_другую_реплику=%d/200 ошибок_сценария=%d",
		totalGET, totalPOST, crossing, failures)
	if failures != 0 || totalGET[0]+totalGET[1] != 2000 || totalPOST[0]+totalPOST[1] != 2200 ||
		totalPOST[0] == 0 || totalPOST[1] == 0 || crossing == 0 {
		t.Error("расхождение с планом нагрузки или отсутствовала смена ответа между репликами")
	}
	time.Sleep(time.Until(poll.ClosesAt))
	reportURL := proxy.URL + "/admin/polls/" + id + "/results"
	final := awaitFinal(t, reportURL)
	if final.ID != id || final.Ballots == nil || *final.Ballots != 2000 {
		t.Fatalf("неверный итог: %+v", final)
	}
	assertChoices(t, final, []int{1800, 200})
	t.Logf("итог: %d бюллетеней, A=1800 B=200, наблюдался через %s после дедлайна", *final.Ballots, time.Since(poll.ClosesAt))
}

type viewerResult struct {
	gets, posts [2]int
	crossed     bool
	latencies   []time.Duration
	err         error
}

func runViewer(ctx context.Context, pollURL, id string, change bool) (sample viewerResult) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		sample.err = err
		return
	}
	client := &http.Client{Jar: jar, Timeout: 8 * time.Second}
	replica, body, err := viewerRequest(ctx, client, http.MethodGet, pollURL, "", http.StatusOK)
	if err != nil {
		sample.err = err
		return
	}
	var question report
	if err := json.Unmarshal(body, &question); err != nil || question.ID != id || len(question.Choices) != 2 {
		sample.err = fmt.Errorf("неожиданный вопрос: %s", body)
		return
	}
	sample.gets[replica]++
	choices := []string{`{"choices":[1]}`}
	if change {
		choices = append(choices, `{"choices":[2]}`)
	}
	first := -1
	for _, choice := range choices {
		start := time.Now()
		replica, body, err = viewerRequest(ctx, client, http.MethodPost, pollURL+"/votes", choice, http.StatusAccepted)
		sample.latencies = append(sample.latencies, time.Since(start))
		if err != nil {
			sample.err = err
			return
		}
		var reply struct{ Status string }
		if err := json.Unmarshal(body, &reply); err != nil || reply.Status != "accepted" {
			sample.err = fmt.Errorf("неожиданный ответ на голос: %s", body)
			return
		}
		sample.posts[replica]++
		sample.crossed = first >= 0 && first != replica
		first = replica
	}
	return
}

func viewerRequest(ctx context.Context, client *http.Client, method, address, body string, want int) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, address, strings.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return 0, nil, err
	}
	if res.StatusCode != want {
		return 0, nil, fmt.Errorf("%s: HTTP %d, ожидали %d, тело %s", method, res.StatusCode, want, data)
	}
	switch res.Header.Get("X-Test-Replica") {
	case "A":
		return 0, data, nil
	case "B":
		return 1, data, nil
	default:
		return 0, nil, fmt.Errorf("балансировщик не указал реплику")
	}
}

func startProcess(t *testing.T, binary, dsn string, brokers []string) *url.URL {
	t.Helper()
	logfile, err := os.CreateTemp(t.TempDir(), "replica-*.log")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { logfile.Close() })
	// Порт назначает ОС. Адрес появляется в логе только после подключения к обеим зависимостям.
	cmd := exec.Command(binary)
	cmd.Env = append(os.Environ(), "HTTP_ADDR=127.0.0.1:0", "DATABASE_URL="+dsn, "KAFKA_BROKERS="+strings.Join(brokers, ","),
		"ADMIN_TOKEN="+adminToken, "POLL_PARTITIONS=4", "POLL_CLOSE_GRACE_PERIOD=5s")
	cmd.Stdout, cmd.Stderr = logfile, logfile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case err := <-stopped:
			if err != nil {
				t.Errorf("завершение реплики: %v", err)
			}
		case <-time.After(25 * time.Second):
			_ = cmd.Process.Kill()
			<-stopped
			t.Error("превышен срок остановки реплики")
		}
	})
	deadline := time.Now().Add(18 * time.Second)
	for {
		logs, err := os.ReadFile(logfile.Name())
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(logs), "\n") {
			if _, port, found := strings.Cut(line, "televote listening on 127.0.0.1:"); found && port != "" {
				address, err := url.Parse("http://127.0.0.1:" + port)
				if err != nil {
					t.Fatal(err)
				}
				return address
			}
		}
		select {
		case err := <-stopped:
			stopped <- err
			t.Fatalf("реплика завершилась при старте: %v\n%s", err, logs)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("реплика не запустилась: %s", logs)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
