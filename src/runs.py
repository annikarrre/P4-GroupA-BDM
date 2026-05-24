from uuid import uuid4

from src.config import (
    CLIP_MODEL_VERSION,
    LLM_MODEL,
    PROMPT_VERSION,
    SBERT_MODEL_VERSION,
)
from src.db import get_conn
from src.utils import get_git_sha


def create_pipeline_run(dag_run_id: str, limit_param: int) -> str:
    run_id = str(uuid4())

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs (
                    run_id,
                    dag_run_id,
                    status,
                    limit_param,
                    git_sha,
                    clip_version,
                    sbert_version,
                    llm_model,
                    prompt_version
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    dag_run_id,
                    "running",
                    limit_param,
                    get_git_sha(),
                    CLIP_MODEL_VERSION,
                    SBERT_MODEL_VERSION,
                    LLM_MODEL,
                    PROMPT_VERSION,
                ),
            )

    return run_id


def finish_pipeline_run(run_id: str, status: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_runs
                SET ended_at = NOW(),
                    status = %s
                WHERE run_id = %s
                """,
                (status, run_id),
            )