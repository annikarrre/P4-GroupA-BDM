from io import BytesIO
import logging

import boto3
import numpy as np
import open_clip
import torch
from PIL import Image
from pgvector.psycopg import register_vector

from src.config import (
    CLIP_ARCH,
    CLIP_MODEL_VERSION,
    CLIP_PRETRAINED,
    MINIO_BUCKET,
    MINIO_KEY,
    MINIO_SECRET,
    MINIO_URL,
)
from src.db import get_conn
from src.utils import sha256_bytes
from src.timing import timed_stage

logger = logging.getLogger(__name__)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_URL,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
    )


@timed_stage("embed_image")
def embed_images(run_id: str, limit: int) -> dict:
    logger.info("run_id=%s stage=embed_image status=started", run_id)

    s3 = get_s3_client()

    model, _, preprocess = open_clip.create_model_and_transforms(
        CLIP_ARCH,
        pretrained=CLIP_PRETRAINED,
    )
    model.eval()

    with get_conn() as conn:
        register_vector(conn)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT screen_id, png_path
                FROM screens_metadata
                WHERE run_id = %s
                ORDER BY screen_id
                LIMIT %s
                """,
                (run_id, limit),
            )
            rows = cur.fetchall()

    images = []
    screen_ids = []
    fingerprints = []

    for screen_id, png_path in rows:
        obj = s3.get_object(Bucket=MINIO_BUCKET, Key=png_path)
        png_bytes = obj["Body"].read()

        image = Image.open(BytesIO(png_bytes)).convert("RGB")
        images.append(preprocess(image))
        screen_ids.append(screen_id)
        fingerprints.append(sha256_bytes(png_bytes))

    if not images:
        return {
            "stage": "embed_image",
            "rows_in": 0,
            "rows_out": 0,
        }

    batch = torch.stack(images)

    with torch.no_grad():
        vectors = model.encode_image(batch)
        vectors = vectors / vectors.norm(dim=-1, keepdim=True)

    vectors_np = vectors.cpu().numpy().astype("float32")

    with get_conn() as conn:
        register_vector(conn)

        with conn.cursor() as cur:
            for screen_id, vector, fingerprint in zip(
                screen_ids,
                vectors_np,
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
                        "open-clip",
                        CLIP_MODEL_VERSION,
                        "image",
                        vector,
                        run_id,
                        fingerprint,
                    ),
                )

        conn.commit()

    logger.info(
        "run_id=%s stage=embed_image status=finished rows_in=%s rows_out=%s",
        run_id,
        len(rows),
        len(screen_ids),
    )

    return {
        "stage": "embed_image",
        "rows_in": len(rows),
        "rows_out": len(screen_ids),
    }