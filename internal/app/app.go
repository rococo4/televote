package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/singleflight"

	"github.com/rococo4/televote/internal/adapter/kafka"
	"github.com/rococo4/televote/internal/api/http/adminpolls"
	"github.com/rococo4/televote/internal/api/http/votes"
	"github.com/rococo4/televote/internal/entity"
	"github.com/rococo4/televote/internal/repository/closurerepo"
	"github.com/rococo4/televote/internal/repository/pollsrepo"
	"github.com/rococo4/televote/internal/repository/resultsrepo"
	"github.com/rococo4/televote/internal/service/counting"
	"github.com/rococo4/televote/internal/service/polls"
	"github.com/rococo4/televote/internal/service/voting"
)

type AdminConfig struct {
	Token      string
	Partitions int
}

type VoteJournal interface {
	EnsureTopic(context.Context, entity.Poll) (uuid.UUID, error)
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

type closureStore interface {
	Join(context.Context, entity.Poll, uuid.UUID) (bool, time.Time, error)
	Drain(context.Context, entity.Poll, uuid.UUID) error
	Decide(context.Context, entity.Poll) (bool, time.Time, error)
}

// App объединяет HTTP-маршруты и фоновые задачи одной реплики сервиса.
type App struct {
	http.Handler
	voting   *voting.Service
	counting *counting.Service
	mu       sync.RWMutex
	polls    map[uuid.UUID]localPoll
	startJob func(entity.Poll)
	loading  singleflight.Group
	source   *pollsrepo.Repository
	writerID uuid.UUID
	closures closureStore
	journal  VoteJournal
}

func New(ctx context.Context, pool *pgxpool.Pool, producer VoteJournal, closeGrace time.Duration, admin AdminConfig) (*App, error) {
	source := pollsrepo.New(pool)
	published, err := source.LoadPublished(ctx)
	if err != nil {
		return nil, err
	}
	if err := producer.Ping(ctx); err != nil {
		return nil, fmt.Errorf("connect to Kafka: %w", err)
	}
	results := resultsrepo.New(pool)
	application := &App{
		voting: voting.New(nil, producer, time.Now), counting: counting.New(producer, results),
		polls: make(map[uuid.UUID]localPoll, len(published)), writerID: uuid.New(),
		closures: closurerepo.New(pool, closeGrace), journal: producer, source: source,
	}
	for _, p := range published {
		if err := application.registerPoll(ctx, p); err != nil {
			return nil, err
		}
	}
	adminService := polls.New(source, producer, admin.Partitions, application.registerPoll)
	mux := http.NewServeMux()
	mux.Handle("/admin/", adminpolls.NewHandler(adminService, admin.Token))
	mux.Handle("/api/", votes.NewHandler(application.voting, application.loadForVote))
	application.Handler = mux
	return application, nil
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
	partitions, err := strconv.ParseInt(setting("POLL_PARTITIONS", "4"), 10, 32)
	if err != nil || partitions < 1 {
		return errors.New("POLL_PARTITIONS must be a positive 32-bit integer")
	}
	application, err := New(startup, pool, producer, closeGrace, AdminConfig{Token: os.Getenv("ADMIN_TOKEN"), Partitions: int(partitions)})
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", setting("HTTP_ADDR", "127.0.0.1:8080"))
	if err != nil {
		return err
	}
	server := &http.Server{
		Handler:           application,
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
		application.ServePolls(countCtx)
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
	application.voting.Drain()
	<-countsDone
	flushErr := producer.Flush(shutdown)
	if flushErr == nil {
		// Реплика завершила все отправки и может поставить drained_at даже до конца опроса.
		application.mu.RLock()
		localPolls := maps.Clone(application.polls)
		application.mu.RUnlock()
		for _, local := range localPolls {
			if local.joined {
				shutdownErr = errors.Join(shutdownErr, application.closures.Drain(shutdown, local.poll, application.writerID))
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
