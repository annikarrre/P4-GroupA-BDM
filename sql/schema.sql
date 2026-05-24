CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS screens_metadata (
    screen_id BIGINT PRIMARY KEY,
    app_package TEXT,
    category TEXT,
    png_path TEXT,
    hierarchy_json_path TEXT
);

CREATE TABLE IF NOT EXISTS screens_embeddings (
    id BIGSERIAL PRIMARY KEY,
    screen_id BIGINT NOT NULL,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    embedding_kind TEXT NOT NULL,
    vector vector
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id UUID PRIMARY KEY,
    dag_run_id TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMP,
    status TEXT NOT NULL,
    limit_param INT NOT NULL,
    git_sha TEXT,
    clip_version TEXT,
    sbert_version TEXT,
    llm_model TEXT,
    prompt_version TEXT
);

CREATE TABLE IF NOT EXISTS audit_results (
    id SERIAL PRIMARY KEY,
    run_id UUID REFERENCES pipeline_runs(run_id),
    audit_name TEXT NOT NULL,
    passed BOOLEAN NOT NULL,
    details JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id SERIAL PRIMARY KEY,
    run_id UUID REFERENCES pipeline_runs(run_id),
    metric_name TEXT NOT NULL,
    metric_value TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE screens_metadata
ADD COLUMN IF NOT EXISTS run_id UUID REFERENCES pipeline_runs(run_id),
ADD COLUMN IF NOT EXISTS source_fingerprint TEXT,
ADD COLUMN IF NOT EXISTS extraction_payload JSONB,
ADD COLUMN IF NOT EXISTS prompt_version TEXT,
ADD COLUMN IF NOT EXISTS confidence DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();

ALTER TABLE screens_embeddings
ADD COLUMN IF NOT EXISTS run_id UUID REFERENCES pipeline_runs(run_id),
ADD COLUMN IF NOT EXISTS source_fingerprint TEXT;

CREATE TABLE IF NOT EXISTS screens_review_queue (
    id SERIAL PRIMARY KEY,
    screen_id INT NOT NULL,
    run_id UUID REFERENCES pipeline_runs(run_id),
    source_fingerprint TEXT,
    reason TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_review_queue_unique
ON screens_review_queue (screen_id, run_id, reason);

CREATE UNIQUE INDEX IF NOT EXISTS idx_screens_metadata_screen_id
ON screens_metadata (screen_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_screens_embeddings_unique
ON screens_embeddings (
    screen_id,
    model_name,
    model_version,
    embedding_kind
);