BEGIN;

-- Each version has a dedicated Kafka log starting at offset 0 in every partition.
-- The Kafka topic UUID detects deletion and recreation under the same name.
CREATE TABLE IF NOT EXISTS poll_count_manifests (
    poll_id    uuid NOT NULL REFERENCES polls (id) ON DELETE CASCADE,
    version    bigint NOT NULL CHECK (version > 0),
    algorithm  integer NOT NULL CHECK (algorithm > 0),
    topic      text NOT NULL,
    topic_id   uuid NOT NULL,
    partitions integer NOT NULL CHECK (partitions > 0),
    PRIMARY KEY (poll_id, version, algorithm)
);

CREATE TABLE IF NOT EXISTS poll_partition_results (
    poll_id      uuid NOT NULL,
    version      bigint NOT NULL,
    algorithm    integer NOT NULL,
    partition_id integer NOT NULL CHECK (partition_id >= 0),
    start_offset bigint NOT NULL CHECK (start_offset = 0),
    end_offset   bigint NOT NULL CHECK (end_offset >= start_offset),
    ballots      bigint NOT NULL CHECK (ballots BETWEEN 0 AND end_offset - start_offset),
    counts       jsonb NOT NULL CHECK (jsonb_typeof(counts) = 'object'),
    PRIMARY KEY (poll_id, version, algorithm, partition_id),
    FOREIGN KEY (poll_id, version, algorithm)
        REFERENCES poll_count_manifests ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS poll_results (
    poll_id   uuid NOT NULL,
    version   bigint NOT NULL,
    algorithm integer NOT NULL,
    ballots   bigint NOT NULL CHECK (ballots >= 0),
    counts    jsonb NOT NULL CHECK (jsonb_typeof(counts) = 'object'),
    PRIMARY KEY (poll_id, version, algorithm),
    FOREIGN KEY (poll_id, version, algorithm)
        REFERENCES poll_count_manifests ON DELETE CASCADE
);

COMMIT;
