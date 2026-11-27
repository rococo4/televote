//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/rococo4/televote/internal/app"
)

// Тест может остановить реплику раньше, например перед проверкой перезапуска.
func startPollWorker(t *testing.T, ctx context.Context, application *app.App) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		application.ServePolls(ctx)
	}()
	stop := func() {
		cancel()
		<-done
	}
	t.Cleanup(stop)
	return stop
}

func waitForFinal(t *testing.T, ctx context.Context, f *voteFixture) {
	t.Helper()
	waitPG(ctx, t, f, `SELECT EXISTS (SELECT 1 FROM poll_results WHERE poll_id=$1)`)
}

func waitPG(ctx context.Context, t *testing.T, f *voteFixture, q string) {
	t.Helper()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		var ok bool
		if err := f.pool.QueryRow(ctx, q, f.poll.ID).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		if ok {
			return
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatalf("timeout waiting for %s: %v", q, ctx.Err())
		}
	}
}
