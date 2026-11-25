package adminpolls

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/rococo4/televote/internal/entity"
)

type Service interface {
	Create(context.Context, entity.CreatePoll) (entity.Poll, error)
	Results(context.Context, uuid.UUID) (entity.PollReport, error)
}

func NewHandler(service Service, token string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/polls", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			var limit *http.MaxBytesError
			if errors.As(err, &limit) {
				writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large")
			} else {
				writeError(w, http.StatusBadRequest, "bad_request")
			}
			return
		}
		input, err := decodeCreate(body)
		if err != nil {
			var syntax *json.SyntaxError
			if errors.As(err, &syntax) {
				writeError(w, http.StatusBadRequest, "bad_request")
			} else {
				writeError(w, http.StatusUnprocessableEntity, "invalid_poll")
			}
			return
		}
		poll, err := service.Create(r.Context(), input)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]uuid.UUID{"id": poll.ID})
	})
	mux.HandleFunc("GET /admin/polls/{id}/results", func(w http.ResponseWriter, r *http.Request) {
		raw := r.PathValue("id")
		id, err := uuid.Parse(raw)
		if err != nil || len(raw) != 36 {
			writeError(w, http.StatusBadRequest, "bad_request")
			return
		}
		report, err := service.Results(r.Context(), id)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, report)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		scheme, credential, _ := strings.Cut(r.Header.Get("Authorization"), " ")
		credential = strings.TrimLeft(credential, " ")
		if token == "" || len(r.Header.Values("Authorization")) != 1 || !strings.EqualFold(scheme, "Bearer") ||
			subtle.ConstantTimeCompare([]byte(token), []byte(credential)) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func decodeCreate(body []byte) (entity.CreatePoll, error) {
	var input entity.CreatePoll
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return input, err
	}
	if raw, ok := fields["id"]; ok {
		var id string
		if json.Unmarshal(raw, &id) != nil || len(id) != 36 {
			return input, entity.ErrInvalidPoll
		}
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil {
			return input, entity.ErrInvalidPoll
		}
		input.ID = parsed
		delete(fields, "id")
	}
	required := map[string]any{
		"question": &input.Question, "choices": &input.Choices,
		"min_choices": &input.MinChoices, "max_choices": &input.MaxChoices,
		"opens_at": &input.OpensAt, "closes_at": &input.ClosesAt,
	}
	if len(fields) != len(required) {
		return input, entity.ErrInvalidPoll
	}
	for name, target := range required {
		value, ok := fields[name]
		if !ok || bytes.Equal(value, []byte("null")) || json.Unmarshal(value, target) != nil {
			return input, entity.ErrInvalidPoll
		}
	}
	return input, nil
}

func writeServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, entity.ErrInvalidPoll):
		writeError(w, http.StatusUnprocessableEntity, "invalid_poll")
	case errors.Is(err, entity.ErrPollConflict):
		writeError(w, http.StatusConflict, "poll_conflict")
	case errors.Is(err, entity.ErrPollNotFound):
		writeError(w, http.StatusNotFound, "poll_not_found")
	default:
		writeError(w, http.StatusServiceUnavailable, "unavailable")
	}
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]string{"code": code})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
