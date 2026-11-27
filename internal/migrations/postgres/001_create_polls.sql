BEGIN;

CREATE TABLE IF NOT EXISTS polls (
    id          uuid PRIMARY KEY,
    question    text NOT NULL,
    opens_at    timestamptz NOT NULL,
    closes_at   timestamptz NOT NULL,
    min_choices integer NOT NULL,
    max_choices integer NOT NULL,
    topic       text NOT NULL UNIQUE,
    partitions  integer NOT NULL,
    published   boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS poll_choices (
    poll_id uuid NOT NULL REFERENCES polls (id) ON DELETE CASCADE,
    id      integer NOT NULL,
    label   text NOT NULL,
    PRIMARY KEY (poll_id, id)
);

COMMIT;
