BEGIN;

CREATE TABLE IF NOT EXISTS poll_closures (
    poll_id         uuid NOT NULL REFERENCES polls(id) ON DELETE CASCADE,
    version         bigint NOT NULL CHECK (version > 0),
    closes_at       timestamptz NOT NULL,
    frozen          boolean NOT NULL DEFAULT false,
    decision_reason text CHECK (decision_reason IN ('drained', 'timeout')),
    decided_at      timestamptz,
    PRIMARY KEY (poll_id, version),
    CHECK ((decision_reason IS NULL) = (decided_at IS NULL)),
    CHECK (decided_at IS NULL OR (frozen AND decided_at >= closes_at))
);

-- Each writer ID identifies one process lifetime. A restart never drains its predecessor.
CREATE TABLE IF NOT EXISTS poll_writers (
    poll_id    uuid NOT NULL,
    version    bigint NOT NULL,
    writer_id  uuid NOT NULL,
    drained_at timestamptz,
    PRIMARY KEY (poll_id, version, writer_id),
    FOREIGN KEY (poll_id, version) REFERENCES poll_closures ON DELETE CASCADE
);

COMMIT;
