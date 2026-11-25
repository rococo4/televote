SELECT p.id, p.question,
       CASE WHEN f.poll_id IS NOT NULL THEN 'final'
            WHEN statement_timestamp() < p.opens_at THEN 'scheduled'
            WHEN statement_timestamp() < p.closes_at THEN 'open'
            ELSE 'finalizing' END,
       f.ballots, c.id, c.label, (f.counts ->> c.id::text)::bigint
FROM polls p
JOIN poll_choices c ON c.poll_id = p.id
LEFT JOIN poll_results f ON f.poll_id = p.id AND f.version = p.version AND f.algorithm = $2
WHERE p.id = $1 AND p.published
ORDER BY c.id
