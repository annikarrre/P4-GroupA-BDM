import json
import logging

from src.db import get_conn

logger = logging.getLogger(__name__)


def save_metric(run_id: str, metric_name: str, metric_value) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_metrics (
                    run_id,
                    metric_name,
                    metric_value
                )
                VALUES (%s, %s, %s)
                """,
                (run_id, metric_name, str(metric_value)),
            )


def collect_metrics(run_id: str) -> dict:
    logger.info("run_id=%s stage=metrics status=started", run_id)

    metrics = {}

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM screens_metadata
                WHERE run_id = %s
                """,
                (run_id,),
            )
            metrics["metadata_row_count"] = cur.fetchone()[0]

            cur.execute(
                """
                SELECT
                    EXTRACT(EPOCH FROM (COALESCE(ended_at, NOW()) - started_at))
                FROM pipeline_runs
                WHERE run_id = %s
                """,
                (run_id,),
            )
            metrics["total_run_duration_seconds"] = round(float(cur.fetchone()[0]), 3)


            cur.execute(
                """
                SELECT status
                FROM pipeline_runs
                WHERE run_id = %s
                """,
                (run_id,),
            )
            metrics["final_run_status"] = cur.fetchone()[0]

            cur.execute(
                """
                SELECT
                    COALESCE(
                        AVG(
                            CASE
                                WHEN extraction_payload IS NOT NULL THEN 1.0
                                ELSE 0.0
                            END
                        ),
                        0
                    )
                FROM screens_metadata
                WHERE run_id = %s
                """,
                (run_id,),
            )
            metrics["metadata_extraction_payload_non_null_pct"] = float(cur.fetchone()[0]) * 100

            cur.execute(
                """
                SELECT
                    COALESCE(
                        AVG(
                            CASE
                                WHEN confidence >= 0.5 THEN 1.0
                                ELSE 0.0
                            END
                        ),
                        0
                    )
                FROM screens_metadata
                WHERE run_id = %s
                """,
                (run_id,),
            )
            metrics["metadata_confidence_gte_0_5_pct"] = float(cur.fetchone()[0]) * 100

            cur.execute(
                """
                SELECT COUNT(*)
                FROM screens_review_queue
                WHERE run_id = %s
                """,
                (run_id,),
            )
            review_count = cur.fetchone()[0]
            metrics["review_queue_row_count"] = review_count

            metadata_count = metrics["metadata_row_count"]
            metrics["metadata_review_queue_pct"] = (
                (review_count / metadata_count) * 100
                if metadata_count
                else 0
            )

            cur.execute(
                """
                SELECT COUNT(DISTINCT app_package)
                FROM screens_metadata
                WHERE run_id = %s
                """,
                (run_id,),
            )
            metrics["distinct_app_package_count"] = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(DISTINCT category)
                FROM screens_metadata
                WHERE run_id = %s
                """,
                (run_id,),
            )
            metrics["distinct_category_count"] = cur.fetchone()[0]

            cur.execute(
                """
                SELECT
                    model_version,
                    embedding_kind,
                    COUNT(*)
                FROM screens_embeddings
                WHERE run_id = %s
                GROUP BY model_version, embedding_kind
                ORDER BY model_version, embedding_kind
                """,
                (run_id,),
            )
            embedding_counts = cur.fetchall()

            for model_version, embedding_kind, count in embedding_counts:
                metric_name = (
                    f"embedding_row_count."
                    f"{model_version}.{embedding_kind}"
                )
                metrics[metric_name] = count

            cur.execute(
                """
                SELECT
                    model_version,
                    embedding_kind,
                    AVG(vector_dims(vector))
                FROM screens_embeddings
                WHERE run_id = %s
                GROUP BY model_version, embedding_kind
                """,
                (run_id,),
            )
            dims = cur.fetchall()

            for model_version, embedding_kind, avg_dim in dims:
                metric_name = (
                    f"embedding_avg_vector_dim."
                    f"{model_version}.{embedding_kind}"
                )
                metrics[metric_name] = float(avg_dim)

            cur.execute(
                """
                SELECT
                    model_version,
                    embedding_kind,
                    COALESCE(
                        AVG(
                            CASE
                                WHEN vector_norm(vector) = 0 THEN 1.0
                                ELSE 0.0
                            END
                        ),
                        0
                    ) * 100
                FROM screens_embeddings
                WHERE run_id = %s
                GROUP BY model_version, embedding_kind
                """,
                (run_id,),
            )
            zero_vectors = cur.fetchall()

            for model_version, embedding_kind, pct in zero_vectors:
                metric_name = (
                    f"embedding_zero_vector_pct."
                    f"{model_version}.{embedding_kind}"
                )
                metrics[metric_name] = float(pct)

    for name, value in metrics.items():
        save_metric(run_id, name, value)

    summary = {
        "run_id": run_id,
        "metadata_rows": metrics.get("metadata_row_count", 0),
        "review_queue_pct": round(
            metrics.get("metadata_review_queue_pct", 0),
            2,
        ),
        "confidence_gte_0_5_pct": round(
            metrics.get("metadata_confidence_gte_0_5_pct", 0),
            2,
        ),
        "distinct_apps": metrics.get("distinct_app_package_count", 0),
        "distinct_categories": metrics.get("distinct_category_count", 0),
    }

    logger.info(
        "run_id=%s stage=metrics status=finished summary=%s",
        run_id,
        json.dumps(summary),
    )

    return {
        "stage": "metrics",
        "metrics": metrics,
        "summary": summary,
    }