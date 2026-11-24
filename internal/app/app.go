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

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rococo4/televote/internal/adapter/kafka"
	"github.com/rococo4/televote/internal/api/http/votes"
	"github.com/rococo4/televote/internal/repository/pollsrepo"
	"github.com/rococo4/televote/internal/service/voting"
)

type Handler struct {
	http.Handler
	voting *voting.Service
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
	return &Handler{Handler: votes.NewHandler(service), voting: service}, nil
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
	select {
	case err = <-stopped:
	case <-ctx.Done():
	}
	shutdown, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	shutdownErr := server.Shutdown(shutdown)
	if shutdownErr != nil {
		_ = server.Close()
	}
	handler.voting.Drain()
	return errors.Join(err, shutdownErr, producer.Flush(shutdown))
}

func setting(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
