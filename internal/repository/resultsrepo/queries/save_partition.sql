INSERT INTO poll_partition_results AS saved
    (poll_id, version, algorithm, partition_id, start_offset, end_offset, ballots, counts)
SELECT poll_id, version, algorithm, $4, $5, $6, $7, $8
FROM poll_count_manifests
WHERE poll_id = $1 AND version = $2 AND algorithm = $3
    AND $4 BETWEEN 0 AND partitions - 1
ON CONFLICT (poll_id, version, algorithm, partition_id) DO UPDATE
    SET end_offset = saved.end_offset
    WHERE (saved.start_offset, saved.end_offset, saved.ballots, saved.counts) =
          (excluded.start_offset, excluded.end_offset, excluded.ballots, excluded.counts)
