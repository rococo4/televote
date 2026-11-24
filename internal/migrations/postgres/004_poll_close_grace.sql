BEGIN;

-- Existing generations were admitted under the original five-second policy.
-- Store whole microseconds so reading the deadline never rounds a large duration.
ALTER TABLE poll_closures
    ADD COLUMN IF NOT EXISTS grace_us bigint NOT NULL DEFAULT 5000000
        CHECK (grace_us BETWEEN 0 AND 9223372036854775);

COMMIT;
