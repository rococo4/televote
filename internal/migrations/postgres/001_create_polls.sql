BEGIN;

CREATE TABLE IF NOT EXISTS polls (
    id          uuid PRIMARY KEY,
    version     bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    question    text NOT NULL CHECK (question <> ''),
    opens_at    timestamptz NOT NULL,
    closes_at   timestamptz NOT NULL,
    min_choices integer NOT NULL CHECK (min_choices >= 1),
    max_choices integer NOT NULL CHECK (max_choices >= min_choices),
    topic       text NOT NULL UNIQUE CHECK (topic <> ''),
    partitions  integer NOT NULL CHECK (partitions >= 1),
    published   boolean NOT NULL DEFAULT false,
    CHECK (opens_at < closes_at)
);

CREATE TABLE IF NOT EXISTS poll_choices (
    poll_id uuid NOT NULL REFERENCES polls (id) ON DELETE CASCADE,
    id      integer NOT NULL,
    label   text NOT NULL CHECK (label <> ''),
    PRIMARY KEY (poll_id, id)
);

COMMIT;
