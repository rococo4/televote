package votes

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/google/uuid"
	"github.com/rococo4/televote/internal/entity"
	"github.com/rococo4/televote/internal/service/voting"
)

const maxBodyBytes = 1 << 20

func NewHandler(service *voting.Service, loadPoll func(context.Context, uuid.UUID) error) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/polls/{id}/votes", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		pollID, err := uuid.Parse(id)
		if err != nil || len(id) != 36 {
			writeError(w, http.StatusBadRequest, "bad_request")
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err != nil {
			var limit *http.MaxBytesError
			if errors.As(err, &limit) {
				writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large")
			} else {
				writeError(w, http.StatusBadRequest, "bad_request")
			}
			return
		}
		admission, err := service.Begin(pollID)
		if errors.Is(err, voting.ErrPollNotFound) && loadPoll != nil {
			if err = loadPoll(r.Context(), pollID); err == nil {
				admission, err = service.Begin(pollID)
			}
		}
		if err != nil {
			writeServiceError(w, err)
			return
		}
		defer admission.Finish()
		token, ballot, err := decodeBallot(body)
		if err != nil {
			var syntax *json.SyntaxError
			if errors.As(err, &syntax) {
				writeError(w, http.StatusBadRequest, "bad_request")
			} else {
				writeError(w, http.StatusUnprocessableEntity, "invalid_ballot")
			}
			return
		}
		if err := service.Submit(r.Context(), admission, token, ballot); err != nil {
			writeServiceError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"status":"accepted"}`)
	})
	return mux
}

func decodeBallot(body []byte) ([16]byte, entity.Ballot, error) {
	var token [16]byte
	var ballot entity.Ballot
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return token, ballot, err
	}
	var encoded string
	if len(fields) != 2 || json.Unmarshal(fields["token"], &encoded) != nil || len(encoded) != hex.EncodedLen(len(token)) {
		return token, ballot, entity.ErrInvalidBallot
	}
	if _, err := hex.Decode(token[:], []byte(encoded)); err != nil {
		return token, ballot, entity.ErrInvalidBallot
	}
	// A JSON null must not be decoded as choice ID zero.
	var choices []*int
	if err := json.Unmarshal(fields["choices"], &choices); err != nil || len(choices) == 0 {
		return token, ballot, entity.ErrInvalidBallot
	}
	ballot.Choices = make([]int, len(choices))
	for i, choice := range choices {
		if choice == nil {
			return token, ballot, entity.ErrInvalidBallot
		}
		ballot.Choices[i] = *choice
	}
	return token, ballot, nil
}

func writeServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, voting.ErrUnavailable):
		writeError(w, http.StatusServiceUnavailable, "unavailable")
	case errors.Is(err, voting.ErrPollNotFound):
		writeError(w, http.StatusNotFound, "poll_not_found")
	case errors.Is(err, entity.ErrNotOpen):
		writeError(w, http.StatusConflict, "not_open")
	case errors.Is(err, entity.ErrClosed):
		writeError(w, http.StatusGone, "closed")
	case errors.Is(err, entity.ErrInvalidBallot):
		writeError(w, http.StatusUnprocessableEntity, "invalid_ballot")
	default:
		writeError(w, http.StatusInternalServerError, "internal_error")
	}
}

func writeError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code})
}
