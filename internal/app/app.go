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
	"github.com/rococo4/televote/internal/repository/pollsrepo"
	"github.com/rococo4/televote/internal/repository/resultsrepo"
	"github.com/rococo4/televote/internal/service/counting"
	"github.com/rococo4/televote/internal/service/voting"
)

type Handler struct {
	http.Handler
	voting   *voting.Service
	counting *counting.Service
	polls    []entity.Poll
}

func NewHandler(ctx context.Context, pool *pgxpool.Pool, producer *kafka.Producer) (*Handler, error) {
	polls, err := pollsrepo.New(pool).LoadPublished(ctx)
	if err != nil {
		return nil, err
	}
	if err := producer.Ping(ctx); err != nil {
		return nil, fmt.Errorf("connect to Kafka: %w", err)
	}
	service := voting.New(polls, producer, time.Now)
	counter := counting.New(producer, resultsrepo.New(pool))
	for _, p := range polls {
		if time.Now().Before(p.ClosesAt) {
			if err := counter.Prepare(ctx, p); err != nil {
				return nil, fmt.Errorf("prepare counting for poll %s: %w", p.ID, err)
			}
		}
	}
	return &Handler{Handler: votes.NewHandler(service), voting: service, counting: counter, polls: polls}, nil
}

// CountClosedPoll resumes a closed poll from its saved partition results.
func (h *Handler) CountClosedPoll(ctx context.Context, id uuid.UUID) error {
	p, err := h.voting.Seal(ctx, id)
	if err != nil {
		return err
	}
	return h.counting.Count(ctx, p)
}

func Run(ctx context.Context) error {
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
	handler, err := NewHandler(startup, pool, producer)
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
	return errors.Join(err, shutdownErr, producer.Flush(shutdown))
}

func setting(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
