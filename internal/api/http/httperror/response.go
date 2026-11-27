package httperror

import (
	"encoding/json"
	"net/http"
)

type response struct {
	status  int
	Code    string `json:"code"`
	Message string `json:"message"`
}

var (
	InvalidJSON          = badRequest("Некорректный JSON.")
	InvalidID            = badRequest("ID опроса должен быть UUID.")
	UnreadableBody       = badRequest("Не удалось прочитать тело запроса.")
	NotOpen              = badRequest("Опрос ещё не открыт.")
	Closed               = badRequest("Опрос закрыт.")
	PayloadTooLarge      = badRequest("Тело запроса превышает 1 МиБ.")
	InvalidBallot        = badRequest("Некорректный бюллетень. Проверьте выбранные варианты.")
	InvalidPoll          = badRequest("Некорректные параметры опроса.")
	PollConflict         = badRequest("Этот ID занят опросом с другими параметрами.")
	UnsupportedMediaType = badRequest("Требуется Content-Type application/json.")
	VoterCookieRequired  = badRequest("Для голосования откройте опрос и разрешите cookie.")
	Unauthorized         = response{http.StatusUnauthorized, "unauthorized", "Требуется действующий токен администратора."}
	PollNotFound         = response{http.StatusNotFound, "poll_not_found", "Опрос не найден."}
	Internal             = response{http.StatusInternalServerError, "internal_error", "Внутренняя ошибка сервера."}
)

func badRequest(message string) response {
	return response{http.StatusBadRequest, "bad_request", message}
}

func (r response) Write(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(r.status)
	_ = json.NewEncoder(w).Encode(r)
}
