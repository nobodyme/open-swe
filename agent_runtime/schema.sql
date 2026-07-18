-- Owned runtime tables (phase-1.md D4 names — binding on Phase 2/3 SQL).
-- Checkpoint/store tables are created by langgraph-checkpoint-postgres's own
-- .setup() and are not defined here.

CREATE TABLE IF NOT EXISTS rt_thread (
  thread_id  UUID PRIMARY KEY,
  status     TEXT NOT NULL DEFAULT 'idle'
             CHECK (status IN ('idle','busy','interrupted','error')),
  metadata   JSONB NOT NULL DEFAULT '{}'::jsonb,
  "values"   JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS rt_thread_status_idx   ON rt_thread (status);
CREATE INDEX IF NOT EXISTS rt_thread_metadata_idx ON rt_thread USING gin (metadata jsonb_path_ops);

CREATE TABLE IF NOT EXISTS rt_run (
  run_id             UUID PRIMARY KEY,
  thread_id          UUID NOT NULL REFERENCES rt_thread(thread_id) ON DELETE CASCADE,
  assistant_id       TEXT NOT NULL,
  status             TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending','running','error','success','timeout','interrupted')),
  multitask_strategy TEXT NOT NULL DEFAULT 'interrupt',
  kwargs             JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata           JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS rt_run_thread_status_idx ON rt_run (thread_id, status);
-- Additive migration: existing databases skip the CREATE TABLE above.
ALTER TABLE rt_run ADD COLUMN IF NOT EXISTS error TEXT;

CREATE TABLE IF NOT EXISTS rt_cron (
  cron_id      UUID PRIMARY KEY,
  assistant_id TEXT NOT NULL,
  thread_id    UUID REFERENCES rt_thread(thread_id) ON DELETE CASCADE,
  schedule     TEXT NOT NULL,
  timezone     TEXT NOT NULL DEFAULT 'UTC',
  end_time     TIMESTAMPTZ,
  payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata     JSONB NOT NULL DEFAULT '{}'::jsonb,
  next_run_date TIMESTAMPTZ,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Event log. `id` is the global replay order (SDK streams; Last-Event-ID);
-- `seq` is the per-thread v2 protocol sequence, assigned ONLY to events that
-- map to a v2 channel (the goldens pin contiguous v2 seqs while the SDK
-- stream also carries non-v2 modes like updates/messages-tuple/checkpoints).
-- Pre-production migration-lite: drop the pre-split-shape table if present.
DO $$ BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'rt_thread_event' AND column_name = 'seq' AND is_nullable = 'NO'
  ) THEN
    DROP TABLE rt_thread_event;
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS rt_thread_event (
  id         BIGSERIAL PRIMARY KEY,
  run_id     UUID NOT NULL REFERENCES rt_run(run_id) ON DELETE CASCADE,
  thread_id  UUID NOT NULL,
  seq        BIGINT,
  event_id   TEXT NOT NULL,
  event      TEXT NOT NULL,
  data       TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS rt_thread_event_thread_idx ON rt_thread_event (thread_id, id);
CREATE INDEX IF NOT EXISTS rt_thread_event_seq_idx    ON rt_thread_event (thread_id, seq);
