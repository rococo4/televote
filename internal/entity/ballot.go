package entity

import (
	"crypto/sha256"
	"time"

	"github.com/google/uuid"
)

type Ballot struct {
	Choices []int
}

type VoteSubmitted struct {
	PollID     uuid.UUID
	Version    int64
	ReceivedAt time.Time
	VoterKey   [32]byte
	Choices    []int
}

func VoterKey(pollID uuid.UUID, token [16]byte) [32]byte {
	var input [32]byte
	copy(input[:16], pollID[:])
	copy(input[16:], token[:])
	return sha256.Sum256(input[:])
}
