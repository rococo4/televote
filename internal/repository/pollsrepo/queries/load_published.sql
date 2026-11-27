SELECT p.id, p.question, p.opens_at, p.closes_at,
       p.min_choices, p.max_choices, p.topic, p.partitions,
       (SELECT COALESCE(jsonb_agg(jsonb_build_object('id', c.id, 'label', c.label) ORDER BY c.id), '[]'::jsonb)
        FROM poll_choices c WHERE c.poll_id = p.id)
FROM polls p
WHERE p.published
