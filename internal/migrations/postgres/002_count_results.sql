BEGIN;

-- Для подсчёта запоминаем, какой топик Kafka был у опроса при публикации.
-- По UUID можно обнаружить, что исходный топик удалили и создали новый под тем же именем.
CREATE TABLE IF NOT EXISTS poll_count_manifests (
    poll_id    uuid PRIMARY KEY REFERENCES polls (id) ON DELETE CASCADE,
    topic      text NOT NULL,
    topic_id   uuid NOT NULL,
    partitions integer NOT NULL
);

CREATE TABLE IF NOT EXISTS poll_partition_results (
    poll_id      uuid NOT NULL REFERENCES poll_count_manifests (poll_id) ON DELETE CASCADE,
    partition_id integer NOT NULL,
    end_offset   bigint NOT NULL,
    ballots      bigint NOT NULL,
    counts       jsonb NOT NULL,
    PRIMARY KEY (poll_id, partition_id)
);

CREATE TABLE IF NOT EXISTS poll_results (
    poll_id uuid PRIMARY KEY REFERENCES poll_count_manifests (poll_id) ON DELETE CASCADE,
    ballots bigint NOT NULL,
    counts  jsonb NOT NULL
);

COMMIT;
