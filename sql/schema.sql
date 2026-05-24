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