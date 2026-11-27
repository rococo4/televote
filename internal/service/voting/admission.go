package voting

import (
	"time"

	"github.com/google/uuid"
	"github.com/rococo4/televote/internal/entity"
)

// Admission фиксирует время приёма запроса. При закрытии опроса сервис ждёт,
// пока обработчик проверит бюллетень и завершит попытку записи в Kafka.
type Admission struct {
	poll       entity.Poll
	receivedAt time.Time
	done       func()
}

// Finish снимает запрос с учёта. Обработчик вызывает его ровно один раз,
// в том числе при ошибке разбора бюллетеня.
func (a *Admission) Finish() { a.done() }

func (s *Service) Begin(pollID uuid.UUID) (*Admission, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if s.draining {
		return nil, ErrUnavailable
	}
	receivedAt := s.now().UTC()
	intake, ok := s.polls[pollID]
	if !ok {
		return nil, ErrPollNotFound
	}
	if intake.sealed != nil {
		return nil, entity.ErrClosed
	}
	if err := intake.poll.CheckWindow(receivedAt); err != nil {
		return nil, err
	}
	intake.inFlight.Add(1)
	return &Admission{poll: intake.poll, receivedAt: receivedAt, done: intake.inFlight.Done}, nil
}

// Seal закрывает приём голосов по опросу. Сервис закрывает канал после того,
// как завершится последний обработчик, который ещё мог отправить голос в Kafka.
func (s *Service) Seal(id uuid.UUID) (<-chan struct{}, error) {
	s.gate.Lock()
	defer s.gate.Unlock()
	intake := s.polls[id]
	if intake == nil {
		return nil, ErrPollNotFound
	}
	if intake.sealed == nil {
		intake.sealed = make(chan struct{})
		go func() {
			intake.inFlight.Wait()
			close(intake.sealed)
		}()
	}
	return intake.sealed, nil
}

// Drain при остановке сервиса закрывает приём голосов по всем опросам
// и ждёт, пока завершатся уже принятые запросы.
func (s *Service) Drain() {
	s.gate.Lock()
	s.draining = true
	s.gate.Unlock()
	for _, intake := range s.polls {
		intake.inFlight.Wait()
	}
}
