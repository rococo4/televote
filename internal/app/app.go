package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rococo4/televote/internal/adapter/kafka"
	"github.com/rococo4/televote/internal/api/http/votes"
	"github.com/rococo4/televote/internal/entity"
	"github.com/rococo4/televote/internal/repository/closurerepo"
	"github.com/rococo4/televote/internal/repository/pollsrepo"
	"github.com/rococo4/televote/internal/repository/resultsrepo"
	"github.com/rococo4/televote/internal/service/counting"
	"github.com/rococo4/televote/internal/service/voting"
)

type VoteJournal interface {
	voting.Publisher
	counting.Journal
	Ping(context.Context) error
	WaitVotes(context.Context, uuid.UUID) error
}

type localPoll struct {
	poll          entity.Poll
	joined        bool
	drainDeadline time.Time
}

type Handler struct {
	http.Handler
	voting   *voting.Service
	counting *counting.Service
	polls    map[uuid.UUID]localPoll
	writerID uuid.UUID
	closures *closurerepo.Repository
	journal  VoteJournal
}

func NewHandler(ctx context.Context, pool *pgxpool.Pool, producer VoteJournal, closeGrace time.Duration) (*Handler, error) {
	polls, err := pollsrepo.New(pool).LoadPublished(ctx)
	if err != nil {
		return nil, err
	}
	if err := producer.Ping(ctx); err != nil {
		return nil, fmt.Errorf("connect to Kafka: %w", err)
	}
	service := voting.New(polls, producer, time.Now)
	counter := counting.New(producer, resultsrepo.New(pool))
	h := &Handler{
		Handler: votes.NewHandler(service), voting: service, counting: counter,
		polls: make(map[uuid.UUID]localPoll, len(polls)), writerID: uuid.New(),
		closures: closurerepo.New(pool, closeGrace), journal: producer,
	}
	for _, p := range polls {
		local := localPoll{poll: p}
		if time.Now().Before(p.ClosesAt) {
			if err := counter.Prepare(ctx, p); err != nil {
				return nil, fmt.Errorf("prepare counting for poll %s: %w", p.ID, err)
			}
			local.joined, local.drainDeadline, err = h.closures.Join(ctx, p, h.writerID)
			if err != nil {
				return nil, err
			}
		}
		if !local.joined {
			if _, err := service.Seal(p.ID); err != nil {
				return nil, err
			}
		}
		h.polls[p.ID] = local
	}
	return h, nil
}

func Run(ctx context.Context) error {
	closeGrace, err := configuredCloseGrace()
	if err != nil {
		return err
	}
	startup, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(startup, setting("DATABASE_URL", "postgres://televote:televote@127.0.0.1:55432/televote?sslmode=disable"))
	if err != nil {
		return fmt.Errorf("configure PostgreSQL: %w", err)
	}
	defer pool.Close()
	brokers := strings.Split(setting("KAFKA_BROKERS", "127.0.0.1:59092"), ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	producer, err := kafka.New(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()
	handler, err := NewHandler(startup, pool, producer, closeGrace)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", setting("HTTP_ADDR", "127.0.0.1:8080"))
	if err != nil {
		return err
	}
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      20 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("televote listening on %s", listener.Addr())
	stopped := make(chan error, 1)
	go func() { stopped <- server.Serve(listener) }()
	countCtx, cancelCounting := context.WithCancel(ctx)
	countsDone := make(chan struct{})
	go func() {
		defer close(countsDone)
		handler.RunCounting(countCtx)
	}()
	select {
	case err = <-stopped:
	case <-ctx.Done():
	}
	cancelCounting()
	shutdown, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	shutdownErr := server.Shutdown(shutdown)
	if shutdownErr != nil {
		_ = server.Close()
	}
	handler.voting.Drain()
	<-countsDone
	flushErr := producer.Flush(shutdown)
	if flushErr == nil {
		// A cleanly stopped writer can acknowledge even before the poll ends.
		for _, local := range handler.polls {
			if local.joined {
				shutdownErr = errors.Join(shutdownErr, handler.closures.Drain(shutdown, local.poll, handler.writerID))
			}
		}
	}
	return errors.Join(err, shutdownErr, flushErr)
}

func setting(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func configuredCloseGrace() (time.Duration, error) {
	const name = "POLL_CLOSE_GRACE_PERIOD"
	value := setting(name, "5s")
	grace, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s=%q: %w", name, value, err)
	}
	if grace < 0 || grace%time.Microsecond != 0 {
		return 0, fmt.Errorf("%s=%q: expected a nonnegative duration in whole microseconds", name, value)
	}
	return grace, nil
}
