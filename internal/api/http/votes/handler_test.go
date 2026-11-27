package votes

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/cookiejar"
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
	const body = `{"choices":[2]}`
	id := pollID.String()
	for _, tc := range []struct {
		name          string
		pollID        string
		body          string
		sinceOpen     time.Duration
		status        int
		code, message string
		publishErr    error
	}{
		{"accepted", id, body, 0, 202, "accepted", "", nil},
		{"malformed JSON", id, body + "{}", 0, 400, "bad_request", "Некорректный JSON.", nil},
		{"malformed UUID", "bad", body, 0, 400, "bad_request", "ID опроса должен быть UUID.", nil},
		{"unpublished", uuid.Nil.String(), body, 0, 404, "poll_not_found", "Опрос не найден.", nil},
		{"not open", id, body, -1, 400, "bad_request", "Опрос ещё не открыт.", nil},
		{"closed", id, body, time.Minute, 400, "bad_request", "Опрос закрыт.", nil},
		{"client token in body", id, `{"token":"00000000000000000000000000000000","choices":[2]}`, 0, 400, "bad_request", "Некорректный бюллетень. Проверьте выбранные варианты.", nil},
		{"unknown choice", id, strings.Replace(body, "[2]", "[3]", 1), 0, 400, "bad_request", "Некорректный бюллетень. Проверьте выбранные варианты.", nil},
		{"repeated choice", id, strings.Replace(body, "[2]", "[2,2]", 1), 0, 400, "bad_request", "Некорректный бюллетень. Проверьте выбранные варианты.", nil},
		{"null choice", id, strings.Replace(body, "[2]", "[null]", 1), 0, 400, "bad_request", "Некорректный бюллетень. Проверьте выбранные варианты.", nil},
		{"string choice", id, strings.Replace(body, "[2]", `["2"]`, 1), 0, 400, "bad_request", "Некорректный бюллетень. Проверьте выбранные варианты.", nil},
		{"missing choices", id, `{}`, 0, 400, "bad_request", "Некорректный бюллетень. Проверьте выбранные варианты.", nil},
		{"extra property", id, strings.Replace(body, "{", `{"extra":42,`, 1), 0, 400, "bad_request", "Некорректный бюллетень. Проверьте выбранные варианты.", nil},
		{"unexpected casing", id, strings.Replace(body, "choices", "Choices", 1), 0, 400, "bad_request", "Некорректный бюллетень. Проверьте выбранные варианты.", nil},
		{"oversize", id, strings.Repeat(" ", maxBodyBytes+1), 0, 400, "bad_request", "Тело запроса превышает 1 МиБ.", nil},
		{"publish error", id, body, 0, 500, "internal_error", "Внутренняя ошибка сервера.", errors.New("no ACK")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			publisher := &recordingPublisher{err: tc.publishErr}
			s := voting.New([]entity.Poll{poll}, publisher, func() time.Time { return start.Add(tc.sinceOpen) })
			req := httptest.NewRequest(http.MethodPost, "/api/polls/"+tc.pollID+"/votes", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			req.AddCookie(&http.Cookie{Name: voterCookieName, Value: strings.Repeat("0", 32)})
			w := httptest.NewRecorder()
			NewHandler(s, nil).ServeHTTP(w, req)
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
			if result[key] != tc.code || result["message"] != tc.message {
				t.Errorf("got %v, want %s=%s and message=%q", result, key, tc.code, tc.message)
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

func TestBrowserIdentity(t *testing.T) {
	now := time.Now().UTC()
	poll := entity.Poll{
		ID: uuid.New(), Question: "Какой вариант?", OpensAt: now.Add(-time.Minute),
		ClosesAt: now.Add(time.Minute), MinChoices: 1, MaxChoices: 1,
		Choices: map[int]struct{}{1: {}, 2: {}},
		Options: []entity.PollChoice{{ID: 1, Label: "A"}, {ID: 2, Label: "B"}},
	}
	other := poll
	other.ID = uuid.New()
	pub := &recordingPublisher{}
	service := voting.New([]entity.Poll{poll, other}, pub, func() time.Time { return now })
	server := httptest.NewServer(NewHandler(service, nil))
	defer server.Close()
	path := server.URL + "/api/polls/" + poll.ID.String()

	request := func(client *http.Client, method, url, body string, want int) (*http.Response, []byte) {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/json; charset=utf-8")
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		data, err := io.ReadAll(res.Body)
		if err != nil || res.StatusCode != want {
			t.Fatalf("%s %s: got %d, want %d; body %s, error %v", method, url, res.StatusCode, want, data, err)
		}
		return res, data
	}
	browser := func() *http.Client {
		jar, err := cookiejar.New(nil)
		if err != nil {
			t.Fatal(err)
		}
		return &http.Client{Jar: jar, Transport: server.Client().Transport}
	}

	// Если ответ на первое открытие потерялся, сервер ещё не получил голоса.
	// Браузер без сохранённой cookie может заново открыть опрос.
	request(server.Client(), "GET", path, "", 200)
	_, data := request(server.Client(), "POST", path+"/votes", `{"choices":[1]}`, http.StatusBadRequest)
	assertBadRequest(t, data, "Для голосования откройте опрос и разрешите cookie.")
	if len(pub.events) != 0 {
		t.Fatalf("vote without cookie: %s, events=%v", data, pub.events)
	}

	alice, bob := browser(), browser()
	first, data := request(alice, "GET", path, "", 200)
	var view entity.PublicPoll
	if err := json.Unmarshal(data, &view); err != nil || view.ID != poll.ID || view.Question != poll.Question ||
		len(view.Choices) != 2 || view.Choices[1].Label != "B" || view.MaxChoices != 1 {
		t.Fatalf("unexpected poll %s; err=%v", data, err)
	}
	if first.Header.Get("Cache-Control") != "private, no-store" || len(first.Cookies()) != 1 {
		t.Fatalf("unexpected headers: %v", first.Header)
	}
	cookie := first.Cookies()[0]
	value, err := hex.DecodeString(cookie.Value)
	if err != nil || len(value) != 16 || cookie.Name != voterCookieName || cookie.Domain != "" ||
		cookie.Path != "/api/polls/"+poll.ID.String() || !cookie.HttpOnly || cookie.Secure ||
		cookie.SameSite != http.SameSiteLaxMode || cookie.Expires.Before(poll.ClosesAt) {
		t.Fatalf("unexpected cookie: %+v", cookie)
	}
	again, _ := request(alice, "GET", path, "", 200)
	if len(again.Cookies()) != 0 {
		t.Fatal("GET replaced an existing voter cookie")
	}
	request(alice, "POST", server.URL+"/api/polls/"+other.ID.String()+"/votes", `{"choices":[1]}`, http.StatusBadRequest)
	second, _ := request(bob, "GET", path, "", 200)
	if len(second.Cookies()) != 1 || second.Cookies()[0].Value == cookie.Value {
		t.Fatal("independent browsers must get different cookies")
	}

	// Повтор после потери ответа и смена выбора должны использовать тот же ключ зрителя.
	request(alice, "POST", path+"/votes", `{"choices":[1]}`, 202)
	request(alice, "POST", path+"/votes", `{"choices":[1]}`, 202)
	// Для ссылки с тем же UUID в другом регистре браузер должен отправить ту же cookie.
	alternate := server.URL + "/api/polls/" + strings.ToUpper(poll.ID.String()) + "/votes"
	request(alice, "POST", alternate, `{"choices":[2]}`, 202)
	request(bob, "POST", path+"/votes", `{"choices":[1]}`, 202)
	if len(pub.events) != 4 || pub.events[0].VoterKey != pub.events[1].VoterKey ||
		pub.events[0].VoterKey != pub.events[2].VoterKey || pub.events[0].VoterKey == pub.events[3].VoterKey ||
		pub.events[2].Choices[0] != 2 {
		t.Fatalf("wrong voter keys or choices: %+v", pub.events)
	}
}

func TestInvalidVoterCookie(t *testing.T) {
	now := time.Now().UTC()
	id := uuid.New()
	poll := entity.Poll{ID: id, OpensAt: now.Add(-time.Minute), ClosesAt: now.Add(time.Minute)}
	pub := &recordingPublisher{}
	h := NewHandler(voting.New([]entity.Poll{poll}, pub, func() time.Time { return now }), nil)
	path := "/api/polls/" + id.String()
	for _, value := range []string{"x", strings.Repeat("x", 32)} {
		req := httptest.NewRequest("POST", path+"/votes", strings.NewReader(`{"choices":[1]}`))
		req.AddCookie(&http.Cookie{Name: voterCookieName, Value: value})
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest || len(pub.events) != 0 {
			t.Fatalf("invalid cookie: status=%d events=%d", rec.Code, len(pub.events))
		}
		assertBadRequest(t, rec.Body.Bytes(), "Для голосования откройте опрос и разрешите cookie.")
		get := httptest.NewRequest("GET", "https://vote.example"+path, nil)
		get.AddCookie(&http.Cookie{Name: voterCookieName, Value: value})
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, get)
		cookies := rec.Result().Cookies()
		if rec.Code != 200 || len(cookies) != 1 || cookies[0].Value == value || !cookies[0].Secure {
			t.Fatalf("expected replacement HTTPS cookie; got %d, %v", rec.Code, cookies)
		}
	}

	proxy := httptest.NewRequest("GET", "http://vote.example"+path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, proxy)
	if len(rec.Result().Cookies()) != 1 || !rec.Result().Cookies()[0].Secure {
		t.Fatal("public host behind TLS proxy must get a Secure cookie")
	}
	req := httptest.NewRequest("POST", path+"/votes", strings.NewReader(`{"choices":[1]}`))
	req.Header.Set("Content-Type", "text/plain")
	req.AddCookie(&http.Cookie{Name: voterCookieName, Value: strings.Repeat("a", 32)})
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || len(pub.events) != 0 {
		t.Fatalf("plain-text ballot: status=%d events=%d", rec.Code, len(pub.events))
	}
	assertBadRequest(t, rec.Body.Bytes(), "Требуется Content-Type application/json.")
}

func assertBadRequest(t *testing.T, body []byte, wantMessage string) {
	t.Helper()
	var response map[string]string
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	if response["code"] != "bad_request" || response["message"] != wantMessage {
		t.Errorf("got %s, want bad_request with message %q", body, wantMessage)
	}
}
