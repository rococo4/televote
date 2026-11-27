//go:build integration

package integration_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/rococo4/televote/internal/entity"
	"github.com/rococo4/televote/internal/service/counting"
)

func TestCountVotesRefusesIncompleteJournal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	f := newVoteFixture(t, ctx, 5*time.Second)
	p := f.poll
	const part = 0
	path := f.server.URL + "/api/polls/" + p.ID.String() + "/votes"
	postVote(t, ctx, f.server.Client(), path, tokenForPartition(p, part), 1)

	time.Sleep(time.Until(p.ClosesAt))
	if err := f.writer.MarkClosed(ctx, p, part); err != nil {
		t.Fatal(err)
	}
	// Имитируем пересоздание топика под прежним именем. Чужой UUID должен остановить
	// подсчёт ещё до обработки первого голоса.
	err := f.writer.ConsumePoll(ctx, p, uuid.New(), counting.GroupCallbacks{
		Progress: func(context.Context) (bool, map[int32]bool, error) {
			return false, nil, nil
		},
		Start: func(int32) (func(entity.VoteSubmitted) error, func(context.Context, int64) error) {
			return func(entity.VoteSubmitted) error { return errors.New("read a vote under the wrong topic UUID") },
				func(context.Context, int64) error { return errors.New("closed the wrong topic UUID") }
		},
	})
	if err == nil || !strings.Contains(err.Error(), "topic identity changed") {
		t.Fatalf("read with wrong topic UUID: %v", err)
	}

	// Имитируем удаление старых записей в Kafka. Подтверждённый голос в позиции 0 исчез,
	// а CLOSE в позиции 1 сохранился. Публиковать результат по такому журналу нельзя.
	trim := kmsg.NewPtrDeleteRecordsRequest()
	trim.Topics = []kmsg.DeleteRecordsRequestTopic{{
		Topic: p.Topic, Partitions: []kmsg.DeleteRecordsRequestTopicPartition{{Partition: part, Offset: 1}},
	}}
	reply, err := trim.RequestWith(ctx, f.reader)
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.Topics) != 1 || len(reply.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected DeleteRecords response: %+v", reply.Topics)
	}
	trimmed := reply.Topics[0].Partitions[0]
	if trimmed.ErrorCode != 0 || trimmed.LowWatermark != 1 {
		t.Fatalf("test partition was not trimmed to offset 1: %+v", trimmed)
	}
	err = f.application.CountClosedPoll(ctx, p.ID)
	if err == nil || !(errors.Is(err, kerr.OffsetOutOfRange) || strings.Contains(err.Error(), "lost range")) {
		t.Fatalf("count after deleting an acknowledged vote: %v", err)
	}
	var published bool
	if err := f.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM poll_results WHERE poll_id=$1)", p.ID).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if published {
		t.Fatal("published an incomplete result after losing an acknowledged vote")
	}
	t.Log("wrong Kafka topic UUID rejected; deleted offset 0, retained CLOSE at 1, no final row in PostgreSQL")
}
