package kafka

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/rococo4/televote/internal/entity"
)

func TestCloseMarkerMustMatchGenerationAndContainNoBallot(t *testing.T) {
	poll := entity.Poll{ID: uuid.New(), Version: 2, Partitions: 4}
	for _, tc := range []struct {
		name    string
		version int
		key     []byte
		fields  string
		close   bool
		invalid bool
	}{
		{name: "earlier poll version", version: 1},
		{name: "current marker", version: 2, close: true},
		{name: "vote key on marker", version: 2, key: make([]byte, 32), invalid: true},
		{name: "choices on marker", version: 2, fields: `,"choices":[2]`, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"type":"CLOSE","poll_id":"%s","version":%d%s}`, poll.ID, tc.version, tc.fields)
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
	poll := entity.Poll{ID: uuid.New(), Version: 1, Partitions: 1}
	for _, tc := range []struct {
		name, choice string
		valid        bool
	}{
		{name: "null", choice: "null"},
		{name: "zero", choice: "0", valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"poll_id":"%s","version":1,"choices":[%s]}`, poll.ID, tc.choice)
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
