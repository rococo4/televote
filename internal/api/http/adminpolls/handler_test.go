package adminpolls

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rococo4/televote/internal/entity"
)

const draftJSON = `{"question":"A or B?","choices":["A","B"],"min_choices":1,"max_choices":1,"opens_at":"2027-01-15T17:00:00Z","closes_at":"2027-01-15T17:01:00Z"}`

type serviceStub struct {
	input   entity.CreatePoll
	askedID uuid.UUID
	report  entity.PollReport
	err     error
	calls   int
}

func (s *serviceStub) Create(_ context.Context, input entity.CreatePoll) (entity.Poll, error) {
	s.calls++
	s.input = input
	return entity.Poll{ID: s.report.ID}, s.err
}

func (s *serviceStub) Results(_ context.Context, id uuid.UUID) (entity.PollReport, error) {
	s.calls++
	s.askedID = id
	return s.report, s.err
}

func request(h http.Handler, method, path, auth, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestAdminRequiresBearerToken(t *testing.T) {
	for _, tc := range []struct{ token, auth string }{
		{"secret", ""}, {"secret", "Basic secret"}, {"secret", "Bearer wrong"},
		{"", "Bearer "}, {"", "Bearer secret"},
	} {
		s := &serviceStub{}
		h := NewHandler(s, tc.token)
		for method, path := range map[string]string{"GET": "/admin/polls/bad/results", "POST": "/admin/polls"} {
			w := request(h, method, path, tc.auth, "")
			if w.Code != 401 || w.Header().Get("WWW-Authenticate") != "Bearer" || s.calls != 0 {
				t.Fatalf("%s %s: got %d, headers %v, service calls %d", method, path, w.Code, w.Header(), s.calls)
			}
		}
	}
}

func TestCreatePollRequest(t *testing.T) {
	id := uuid.New()
	for _, clientID := range []uuid.UUID{uuid.Nil, id} {
		body := draftJSON
		if clientID != uuid.Nil {
			body = strings.Replace(body, "{", fmt.Sprintf(`{"id":%q,`, clientID), 1)
		}
		s := &serviceStub{report: entity.PollReport{ID: id}}
		w := request(NewHandler(s, "secret"), "POST", "/admin/polls", "bearer secret", body)
		var result struct{ ID uuid.UUID }
		if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.ID != id {
			t.Fatalf("POST returned %d %s", w.Code, w.Body.String())
		}
		input := s.input
		if input.ID != clientID || input.Question != "A or B?" || !slices.Equal(input.Choices, []string{"A", "B"}) ||
			input.MinChoices != 1 || input.MaxChoices != 1 ||
			input.OpensAt.Format(time.RFC3339) != "2027-01-15T17:00:00Z" || input.ClosesAt.Sub(input.OpensAt) != time.Minute {
			t.Fatalf("unexpected service input: %+v", input)
		}
	}
}

func TestAdminErrors(t *testing.T) {
	id := uuid.New()
	results := "/admin/polls/" + id.String() + "/results"
	for _, tc := range []struct {
		name, method, path, body string
		err                      error
		status                   int
		code, message            string
	}{
		{"malformed JSON", "POST", "/admin/polls", draftJSON + "{}", nil, 400, "bad_request", "Некорректный JSON."},
		{"missing fields", "POST", "/admin/polls", "{}", nil, 400, "bad_request", "Некорректные параметры опроса."},
		{"unknown field", "POST", "/admin/polls", strings.Replace(draftJSON, "{", `{"extra":1,`, 1), nil, 400, "bad_request", "Некорректные параметры опроса."},
		{"missing timestamp", "POST", "/admin/polls", strings.Replace(draftJSON, `"2027-01-15T17:00:00Z"`, "null", 1), nil, 400, "bad_request", "Некорректные параметры опроса."},
		{"invalid id", "POST", "/admin/polls", strings.Replace(draftJSON, "{", `{"id":"bad",`, 1), nil, 400, "bad_request", "Некорректные параметры опроса."},
		{"oversize", "POST", "/admin/polls", strings.Repeat(" ", 1<<20+1), nil, 400, "bad_request", "Тело запроса превышает 1 МиБ."},
		{"business rule", "POST", "/admin/polls", draftJSON, entity.ErrInvalidPoll, 400, "bad_request", "Некорректные параметры опроса."},
		{"conflicting id", "POST", "/admin/polls", draftJSON, fmt.Errorf("create: %w", entity.ErrPollConflict), 400, "bad_request", "Этот ID занят опросом с другими параметрами."},
		{"malformed path", "GET", "/admin/polls/bad/results", "", nil, 400, "bad_request", "ID опроса должен быть UUID."},
		{"unknown poll", "GET", results, "", entity.ErrPollNotFound, 404, "poll_not_found", "Опрос не найден."},
		{"creation failure", "POST", "/admin/polls", draftJSON, errors.New("database unavailable"), 500, "internal_error", "Внутренняя ошибка сервера."},
		{"dependency failure", "GET", results, "", errors.New("database unavailable"), 500, "internal_error", "Внутренняя ошибка сервера."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &serviceStub{err: tc.err}
			w := request(NewHandler(s, "secret"), tc.method, tc.path, "Bearer secret", tc.body)
			var problem struct{ Code, Message string }
			if w.Code != tc.status || json.Unmarshal(w.Body.Bytes(), &problem) != nil || problem.Code != tc.code || problem.Message != tc.message {
				t.Fatalf("got %d %s, want %d %s", w.Code, w.Body.String(), tc.status, tc.code)
			}
			if (tc.err == nil) != (s.calls == 0) {
				t.Errorf("unexpected number of service calls: %d", s.calls)
			}
		})
	}
}

func TestReportOmitsCountsUntilFinal(t *testing.T) {
	id := uuid.New()
	for _, status := range []string{"scheduled", "open", "finalizing", "final"} {
		t.Run(status, func(t *testing.T) {
			report := entity.PollReport{ID: id, Question: "A or B?", Status: status, Choices: []entity.ChoiceResult{{ID: 1, Label: "A"}}}
			if status == "final" {
				report.Ballots = new(int64)
				report.Choices[0].Votes = new(int64)
			}
			s := &serviceStub{report: report}
			w := request(NewHandler(s, "secret"), "GET", "/admin/polls/"+id.String()+"/results", "Bearer secret", "")
			var got struct {
				Status  string
				Ballots json.RawMessage
				Choices []map[string]json.RawMessage
			}
			if w.Code != 200 || s.askedID != id || json.Unmarshal(w.Body.Bytes(), &got) != nil || len(got.Choices) != 1 || got.Status != status {
				t.Fatalf("got %d %s for %s", w.Code, w.Body.String(), s.askedID)
			}
			choice := got.Choices[0]
			if status == "final" {
				if string(got.Ballots) != "0" || string(choice["votes"]) != "0" || string(choice["percent"]) != "null" {
					t.Errorf("empty final: %s", w.Body.String())
				}
			} else if got.Ballots != nil || choice["votes"] != nil || choice["percent"] != nil {
				t.Errorf("premature counts: %s", w.Body.String())
			}
		})
	}
}
