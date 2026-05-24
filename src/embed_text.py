import logging

import boto3
from pgvector.psycopg import register_vector
from sentence_transformers import SentenceTransformer

from src.config import (
    MINIO_BUCKET,
    MINIO_KEY,
    MINIO_SECRET,
    MINIO_URL,
    SBERT_MODEL_VERSION,
)
from src.db import get_conn
from src.parse import parse_hierarchy, text_representation
from src.timing import timed_stage
from src.utils import sha256_text

logger = logging.getLogger(__name__)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_URL,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
    )


@timed_stage("ingest")
def embed_text(run_id: str, limit: int) -> dict:
    logger.info("run_id=%s stage=embed_text status=started", run_id)

    s3 = get_s3_client()
    sbert = SentenceTransformer(SBERT_MODEL_VERSION)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT screen_id, hierarchy_json_path
                FROM screens_metadata
                WHERE run_id = %s
                ORDER BY screen_id
                LIMIT %s
                """,
                (run_id, limit),
            )
            rows = cur.fetchall()

    screen_ids = []
    text_reps = []
    fingerprints = []

    for screen_id, hierarchy_json_path in rows:
        raw = (
            s3.get_object(Bucket=MINIO_BUCKET, Key=hierarchy_json_path)["Body"]
            .read()
            .decode("utf-8")
        )

        text = text_representation(parse_hierarchy(raw))

        screen_ids.append(screen_id)
        text_reps.append(text)
        fingerprints.append(sha256_text(text))

    if not text_reps:
        return {
            "stage": "embed_text",
            "rows_in": 0,
            "rows_out": 0,
        }

    vectors = sbert.encode(
        text_reps,
        normalize_embeddings=True,
    ).astype("float32")

    with get_conn() as conn:
        register_vector(conn)

        with conn.cursor() as cur:
            for screen_id, vector, fingerprint in zip(
                screen_ids,
                vectors,
                fingerprints,
                strict=True,
            ):
                cur.execute(
                    """
                    INSERT INTO screens_embeddings (
                        screen_id,
                        model_name,
                        model_version,
                        embedding_kind,
                        vector,
                        run_id,
                        source_fingerprint
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (
                        screen_id,
                        model_name,
                        model_version,
                        embedding_kind
                    )
                    DO UPDATE SET
                        vector = EXCLUDED.vector,
                        run_id = EXCLUDED.run_id,
                        source_fingerprint = EXCLUDED.source_fingerprint
                    """,
                    (
                        screen_id,
                        "sentence-transformers",
                        SBERT_MODEL_VERSION,
                        "text",
                        vector,
                        run_id,
                        fingerprint,
                    ),
                )

        conn.commit()

    logger.info(
        "run_id=%s stage=embed_text status=finished rows_in=%s rows_out=%s",
        run_id,
        len(rows),
        len(screen_ids),
    )

    return {
        "stage": "embed_text",
        "rows_in": len(rows),
        "rows_out": len(screen_ids),
    }