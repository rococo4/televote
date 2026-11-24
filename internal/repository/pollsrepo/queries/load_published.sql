SELECT p.id, p.version, p.opens_at, p.closes_at,
       p.min_choices, p.max_choices, p.topic, p.partitions,
       ARRAY(SELECT c.id FROM poll_choices c WHERE c.poll_id = p.id ORDER BY c.id)
FROM polls p
WHERE p.published
ORDER BY p.id
