package votes

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rococo4/televote/internal/api/http/httperror"
	"github.com/rococo4/televote/internal/entity"
	"github.com/rococo4/televote/internal/service/voting"
)

const maxBodyBytes = 1 << 20
const voterCookieName = "televote_voter"

func NewHandler(service *voting.Service, loadPoll func(context.Context, uuid.UUID) error) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/polls/{id}", pollEndpoint("", func(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
		poll, err := service.PublicPoll(id)
		if errors.Is(err, voting.ErrPollNotFound) && loadPoll != nil {
			if err = loadPoll(r.Context(), id); err == nil {
				poll, err = service.PublicPoll(id)
			}
		}
		if err != nil {
			writeServiceError(w, err)
			return
		}
		if _, ok := voterToken(r); !ok && time.Now().Before(poll.ClosesAt) {
			var identity [16]byte
			rand.Read(identity[:])
			http.SetCookie(w, &http.Cookie{
				Name: voterCookieName, Value: hex.EncodeToString(identity[:]),
				Path: "/api/polls/" + id.String(), HttpOnly: true,
				SameSite: http.SameSiteLaxMode, Secure: secureCookie(r),
				// HTTP-дата имеет точность до секунды. Cookie нужна до конца голосования.
				Expires: poll.ClosesAt.Add(time.Second - time.Nanosecond).UTC().Truncate(time.Second),
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(poll)
	}))
	mux.HandleFunc("POST /api/polls/{id}/votes", pollEndpoint("/votes", func(w http.ResponseWriter, r *http.Request, pollID uuid.UUID) {
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "application/json" {
			httperror.UnsupportedMediaType.Write(w)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err != nil {
			var limit *http.MaxBytesError
			if errors.As(err, &limit) {
				httperror.PayloadTooLarge.Write(w)
			} else {
				httperror.UnreadableBody.Write(w)
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
		token, ok := voterToken(r)
		if !ok {
			httperror.VoterCookieRequired.Write(w)
			return
		}
		ballot, err := decodeBallot(body)
		if err != nil {
			var syntax *json.SyntaxError
			if errors.As(err, &syntax) {
				httperror.InvalidJSON.Write(w)
			} else {
				httperror.InvalidBallot.Write(w)
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
	}))
	return mux
}

func pollEndpoint(suffix string, serve func(http.ResponseWriter, *http.Request, uuid.UUID)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "private, no-store")
		raw := r.PathValue("id")
		id, err := uuid.Parse(raw)
		if err != nil || len(raw) != 36 {
			httperror.InvalidID.Write(w)
			return
		}
		// Браузер сравнивает пути cookie посимвольно. У одного опроса должен быть
		// один путь, в том числе при переходе по ссылке с UUID в верхнем регистре.
		canonical := "/api/polls/" + id.String() + suffix
		if r.URL.EscapedPath() != canonical {
			if r.URL.RawQuery != "" {
				canonical += "?" + r.URL.RawQuery
			}
			http.Redirect(w, r, canonical, http.StatusTemporaryRedirect)
			return
		}
		serve(w, r, id)
	}
}

func voterToken(r *http.Request) ([16]byte, bool) {
	var token [16]byte
	c, err := r.Cookie(voterCookieName)
	if err != nil || len(c.Value) != hex.EncodedLen(len(token)) {
		return token, false
	}
	_, err = hex.Decode(token[:], []byte(c.Value))
	return token, err == nil
}

func secureCookie(r *http.Request) bool {
	host := (&url.URL{Host: r.Host}).Hostname()
	// За балансировщиком соединение с Go может быть обычным HTTP.
	// Исключение нужно только для локального запуска без TLS.
	return r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") ||
		!(strings.EqualFold(host, "localhost") || net.ParseIP(host).IsLoopback())
}

func decodeBallot(body []byte) (entity.Ballot, error) {
	var ballot entity.Ballot
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return ballot, err
	}
	if len(fields) != 1 {
		return ballot, entity.ErrInvalidBallot
	}
	// Указатель позволяет отклонить null и при этом принять вариант с ID 0.
	var choices []*int
	if err := json.Unmarshal(fields["choices"], &choices); err != nil || len(choices) == 0 {
		return ballot, entity.ErrInvalidBallot
	}
	ballot.Choices = make([]int, len(choices))
	for i, choice := range choices {
		if choice == nil {
			return ballot, entity.ErrInvalidBallot
		}
		ballot.Choices[i] = *choice
	}
	return ballot, nil
}

func writeServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, voting.ErrPollNotFound):
		httperror.PollNotFound.Write(w)
	case errors.Is(err, entity.ErrNotOpen):
		httperror.NotOpen.Write(w)
	case errors.Is(err, entity.ErrClosed):
		httperror.Closed.Write(w)
	case errors.Is(err, entity.ErrInvalidBallot):
		httperror.InvalidBallot.Write(w)
	default:
		httperror.Internal.Write(w)
	}
}
