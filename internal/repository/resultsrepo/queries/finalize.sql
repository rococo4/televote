-- Сверяем набор партиций и суммируем их итоги в одном снимке данных PostgreSQL.
WITH parts AS (
    SELECT partition_id, ballots, counts
    FROM poll_partition_results
    WHERE poll_id = $1
), choices AS (
    SELECT choice.key, sum(choice.value::bigint)::bigint AS votes
    FROM parts CROSS JOIN LATERAL jsonb_each_text(parts.counts) AS choice
    GROUP BY choice.key
)
INSERT INTO poll_results AS saved (poll_id, ballots, counts)
SELECT m.poll_id, sum(p.ballots)::bigint,
       (SELECT jsonb_object_agg(key, votes) FROM choices)
FROM poll_count_manifests m CROSS JOIN parts p
WHERE m.poll_id = $1
GROUP BY m.poll_id
-- Номера партиций уникальны, поэтому по количеству и крайним номерам можно проверить,
-- что собраны итоги всех партиций от 0 до P − 1.
HAVING count(*) = m.partitions AND min(p.partition_id) = 0 AND max(p.partition_id) = m.partitions - 1
ON CONFLICT (poll_id) DO UPDATE
    SET ballots = saved.ballots
    WHERE (saved.ballots, saved.counts) = (excluded.ballots, excluded.counts)
