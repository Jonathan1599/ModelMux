CREATE TABLE IF NOT EXISTS api_keys (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  key_prefix VARCHAR(16) NOT NULL,
  key_hash CHAR(64) NOT NULL UNIQUE,
  rate_limit_capacity INTEGER NOT NULL CHECK (rate_limit_capacity > 0),
  rate_limit_refill_per_second DOUBLE PRECISION NOT NULL
    CHECK (rate_limit_refill_per_second > 0),
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
