package kafka

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/rococo4/televote/internal/entity"
)

func TestCloseMarkerMustMatchPollAndContainNoBallot(t *testing.T) {
	poll := entity.Poll{ID: uuid.New(), Partitions: 4}
	for _, tc := range []struct {
		name    string
		pollID  uuid.UUID
		key     []byte
		fields  string
		close   bool
		invalid bool
	}{
		{name: "other poll", pollID: uuid.New()},
		{name: "missing poll ID", invalid: true},
		{name: "current marker", pollID: poll.ID, close: true},
		{name: "vote key on marker", pollID: poll.ID, key: make([]byte, 32), invalid: true},
		{name: "choices on marker", pollID: poll.ID, fields: `,"choices":[2]`, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"type":"CLOSE","poll_id":"%s"%s}`, tc.pollID, tc.fields)
			closed, err := readEvent(poll, &kgo.Record{Key: tc.key, Value: []byte(body)}, func(entity.VoteSubmitted) error {
				t.Fatal("marker decoded as a vote")
				return nil
			})
			if closed != tc.close || (err != nil) != tc.invalid {
				t.Fatalf("closed=%v error=%v, want closed=%v invalid=%v", closed, err, tc.close, tc.invalid)
			}
		})
	}
}

func TestReplayDistinguishesNullFromZeroChoice(t *testing.T) {
	poll := entity.Poll{ID: uuid.New(), Partitions: 1}
	for _, tc := range []struct {
		name, choice string
		valid        bool
	}{
		{name: "null", choice: "null"},
		{name: "zero", choice: "0", valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"poll_id":"%s","choices":[%s]}`, poll.ID, tc.choice)
			var visited bool
			closed, err := readEvent(poll, &kgo.Record{Key: make([]byte, 32), Value: []byte(body)}, func(v entity.VoteSubmitted) error {
				visited = true
				if len(v.Choices) != 1 || v.Choices[0] != 0 {
					t.Errorf("choices=%v, want [0]", v.Choices)
				}
				return nil
			})
			if err != nil || closed || visited != tc.valid {
				t.Fatalf("visited=%v closed=%v err=%v, want visited=%v", visited, closed, err, tc.valid)
			}
		})
	}
}
