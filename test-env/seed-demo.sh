#!/bin/sh
set -eu
cd "$(dirname "$0")"

docker compose -f compose.yaml exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic televote-demo --partitions 4 --replication-factor 1

docker compose -f compose.yaml exec -T postgres psql -X -v ON_ERROR_STOP=1 -U televote -d televote <<'SQL'
BEGIN;
INSERT INTO polls (id, question, opens_at, closes_at, min_choices, max_choices, topic, partitions, published)
VALUES ('c17c57c4-51b1-4d95-841f-1e44356f5608', 'A или B?', now(), now() + interval '1 hour',
        1, 1, 'televote-demo', 4, true)
ON CONFLICT (id) DO UPDATE
    SET opens_at = excluded.opens_at, closes_at = excluded.closes_at, version = polls.version + 1;
INSERT INTO poll_choices (poll_id, id, label) VALUES
    ('c17c57c4-51b1-4d95-841f-1e44356f5608', 1, 'A'),
    ('c17c57c4-51b1-4d95-841f-1e44356f5608', 2, 'B')
ON CONFLICT (poll_id, id) DO NOTHING;
COMMIT;
SQL
