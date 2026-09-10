-- Adds the token fields used by the worker and the per-tenant metrics endpoint.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS input_tokens INTEGER;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS output_tokens INTEGER;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS total_tokens INTEGER;

-- These indexes keep tenant history and daily metrics queries efficient as the jobs table grows.
CREATE INDEX IF NOT EXISTS jobs_user_created_idx ON jobs (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS jobs_status_created_idx ON jobs (status, created_at);
