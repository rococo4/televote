package entity

// PartitionResult хранит итог одной партиции по записям до первого маркера CLOSE.
// EndOffset — позиция этого маркера.
type PartitionResult struct {
	Partition int32
	EndOffset int64
	Ballots   int64
	Counts    map[int]int64
}
