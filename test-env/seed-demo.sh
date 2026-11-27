#!/bin/sh
set -eu
cd "$(dirname "$0")"

db() {
    docker compose -f compose.yaml exec -T postgres psql -X -q -v ON_ERROR_STOP=1 -U televote -d televote "$@"
}

poll_id=$(db -Atc 'SELECT gen_random_uuid()')
topic="televote-$poll_id"
docker compose -f compose.yaml exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 --create --topic "$topic" \
    --partitions 4 --replication-factor 1 --config cleanup.policy=delete >&2

db -v poll_id="$poll_id" -v topic="$topic" <<'SQL'
BEGIN;
INSERT INTO polls (id, question, opens_at, closes_at, min_choices, max_choices, topic, partitions, published)
VALUES (:'poll_id', 'A или B?', now(), now() + interval '1 hour', 1, 1, :'topic', 4, true);
INSERT INTO poll_choices (poll_id, id, label) VALUES
    (:'poll_id', 1, 'A'), (:'poll_id', 2, 'B');
COMMIT;
SQL
printf '%s\n' "$poll_id"
