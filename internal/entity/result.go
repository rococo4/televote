package entity

// PartitionResult describes one complete pass up to the first CLOSE.
// EndOffset is the position of the marker, excluded from the ballot range.
type PartitionResult struct {
	Partition   int32
	StartOffset int64
	EndOffset   int64
	Ballots     int64
	Counts      map[int]int64
}
