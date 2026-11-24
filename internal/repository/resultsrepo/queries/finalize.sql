-- Read the manifest and all absolute partial totals in one statement snapshot.
WITH parts AS (
    SELECT partition_id, ballots, counts
    FROM poll_partition_results
    WHERE poll_id = $1 AND version = $2 AND algorithm = $3
), choices AS (
    SELECT choice.key, sum(choice.value::bigint)::bigint AS votes
    FROM parts CROSS JOIN LATERAL jsonb_each_text(parts.counts) AS choice
    GROUP BY choice.key
)
INSERT INTO poll_results AS saved (poll_id, version, algorithm, ballots, counts)
SELECT m.poll_id, m.version, m.algorithm, sum(p.ballots)::bigint,
       (SELECT jsonb_object_agg(key, votes) FROM choices)
FROM poll_count_manifests m CROSS JOIN parts p
WHERE m.poll_id = $1 AND m.version = $2 AND m.algorithm = $3
GROUP BY m.poll_id, m.version, m.algorithm
-- Partition ids are unique. These bounds and cardinality establish 0..P-1.
HAVING count(*) = m.partitions AND min(p.partition_id) = 0 AND max(p.partition_id) = m.partitions - 1
ON CONFLICT (poll_id, version, algorithm) DO UPDATE
    SET ballots = saved.ballots
    WHERE (saved.ballots, saved.counts) = (excluded.ballots, excluded.counts)
