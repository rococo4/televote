BEGIN;

CREATE TABLE IF NOT EXISTS poll_closures (
    poll_id         uuid PRIMARY KEY REFERENCES polls (id) ON DELETE CASCADE,
    closes_at       timestamptz NOT NULL,
    grace_us        bigint NOT NULL DEFAULT 5000000,
    frozen          boolean NOT NULL DEFAULT false,
    decision_reason text,
    decided_at      timestamptz
);

-- При перезапуске реплика получает новый writer_id, чтобы её отметка drained_at
-- подтверждала только отправки текущего процесса.
CREATE TABLE IF NOT EXISTS poll_writers (
    poll_id    uuid NOT NULL REFERENCES poll_closures (poll_id) ON DELETE CASCADE,
    writer_id  uuid NOT NULL,
    drained_at timestamptz,
    PRIMARY KEY (poll_id, writer_id)
);

COMMIT;
