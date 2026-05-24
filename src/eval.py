import logging

from src.db import get_conn
from src.metrics import save_metric
from src.timing import timed_stage

logger = logging.getLogger(__name__)

@timed_stage("ingest")
def run_eval(run_id: str) -> dict:
    """
    For now, we store whether the run has both image and text embeddings.
    """
    logger.info("run_id=%s stage=eval status=started", run_id)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(DISTINCT screen_id)
                FROM screens_embeddings
                WHERE run_id = %s
                  AND embedding_kind = 'image'
                """,
                (run_id,),
            )
            image_count = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(DISTINCT screen_id)
                FROM screens_embeddings
                WHERE run_id = %s
                  AND embedding_kind = 'text'
                """,
                (run_id,),
            )
            text_count = cur.fetchone()[0]

    denominator = max(image_count, text_count, 1)
    paired_embedding_pct = (
        min(image_count, text_count) / denominator
    ) * 100

    save_metric(run_id, "eval_paired_embedding_pct", paired_embedding_pct)

    logger.info(
        "run_id=%s stage=eval status=finished paired_embedding_pct=%s",
        run_id,
        paired_embedding_pct,
    )

    return {
        "stage": "eval",
        "image_count": image_count,
        "text_count": text_count,
        "paired_embedding_pct": paired_embedding_pct,
    }