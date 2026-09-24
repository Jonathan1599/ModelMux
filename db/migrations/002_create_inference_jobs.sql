CREATE TABLE IF NOT EXISTS inference_jobs (
  id TEXT PRIMARY KEY,
  queue_name TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  idempotency_hash CHAR(64) NOT NULL,
  payload_hash CHAR(64) NOT NULL,
  request_id TEXT NOT NULL,
  data JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued'
    CHECK (status IN ('queued', 'active', 'retrying', 'completed', 'failed')),
  attempts_made INTEGER NOT NULL DEFAULT 0 CHECK (attempts_made >= 0),
  max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
  backoff_ms INTEGER NOT NULL CHECK (backoff_ms > 0),
  response JSONB,
  error_code TEXT,
  dispatch_pending BOOLEAN NOT NULL DEFAULT TRUE,
  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  lease_until TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ,
  UNIQUE (queue_name, owner_id, idempotency_hash)
);

CREATE INDEX IF NOT EXISTS inference_jobs_dispatch_idx
  ON inference_jobs (queue_name, available_at)
  WHERE dispatch_pending = TRUE;

CREATE INDEX IF NOT EXISTS inference_jobs_lease_idx
  ON inference_jobs (queue_name, lease_until)
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS inference_jobs_finished_idx
  ON inference_jobs (queue_name, finished_at)
  WHERE finished_at IS NOT NULL;
