INSERT INTO poll_partition_results AS saved
    (poll_id, partition_id, end_offset, ballots, counts)
SELECT poll_id, $2, $3, $4, $5
FROM poll_count_manifests
WHERE poll_id = $1
    AND $2 BETWEEN 0 AND partitions - 1
ON CONFLICT (poll_id, partition_id) DO UPDATE
    SET end_offset = saved.end_offset
    WHERE (saved.end_offset, saved.ballots, saved.counts) =
          (excluded.end_offset, excluded.ballots, excluded.counts)
