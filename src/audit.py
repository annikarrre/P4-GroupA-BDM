import json
import logging

from src.db import get_conn
from src.timing import timed_stage

logger = logging.getLogger(__name__)


class AuditFailedError(Exception):
    pass

@timed_stage("audit")
def run_duplicate_audit(run_id: str) -> dict:
    logger.info("run_id=%s stage=audit status=started", run_id)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT screen_id, COUNT(*) AS count
                FROM screens_metadata
                WHERE run_id = %s
                GROUP BY screen_id
                HAVING COUNT(*) > 1
                """,
                (run_id,),
            )
            metadata_duplicates = cur.fetchall()

            cur.execute(
                """
                SELECT
                    screen_id,
                    model_name,
                    model_version,
                    embedding_kind,
                    COUNT(*) AS count
                FROM screens_embeddings
                GROUP BY
                    screen_id,
                    model_name,
                    model_version,
                    embedding_kind
                HAVING COUNT(*) > 1
                """
            )
            embedding_duplicates = cur.fetchall()

            details = {
                "metadata_duplicates": [
                    {"screen_id": row[0], "count": row[1]}
                    for row in metadata_duplicates
                ],
                "embedding_duplicates": [
                    {
                        "screen_id": row[0],
                        "model_name": row[1],
                        "model_version": row[2],
                        "embedding_kind": row[3],
                        "count": row[4],
                    }
                    for row in embedding_duplicates
                ],
            }

            passed = (
                len(metadata_duplicates) == 0
                and len(embedding_duplicates) == 0
            )

            cur.execute(
                """
                INSERT INTO audit_results (
                    run_id,
                    audit_name,
                    passed,
                    details
                )
                VALUES (%s, %s, %s, %s::jsonb)
                """,
                (
                    run_id,
                    "duplicate_detection",
                    passed,
                    json.dumps(details),
                ),
            )

        conn.commit()

    if not passed:
        logger.error(
            "run_id=%s stage=audit status=failed duplicate_keys=%s",
            run_id,
            json.dumps(details),
        )
        raise AuditFailedError(
            f"Duplicate audit failed for run_id={run_id}: {details}"
        )

    logger.info("run_id=%s stage=audit status=passed", run_id)

    return {
        "stage": "audit",
        "passed": True,
        "details": details,
    }