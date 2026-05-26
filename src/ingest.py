from io import BytesIO
import itertools
import logging

import boto3
from datasets import load_dataset

from src.config import (
    MINIO_BUCKET,
    MINIO_KEY,
    MINIO_SECRET,
    MINIO_URL,
)
from src.db import get_conn
from src.utils import sha256_bytes, sha256_text
from src.timing import timed_stage

logger = logging.getLogger(__name__)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_URL,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
    )


def ensure_bucket_exists(s3) -> None:
    existing = [
        bucket["Name"]
        for bucket in s3.list_buckets().get("Buckets", [])
    ]

    if MINIO_BUCKET not in existing:
        s3.create_bucket(Bucket=MINIO_BUCKET)


@timed_stage("ingest")
def ingest_screens(run_id: str, limit: int) -> dict:
    """
    Stream RICO rows, upload PNG + hierarchy JSON to MinIO,
    and upsert metadata rows into Postgres.

    Idempotency:
        - MinIO object keys are deterministic: screens/{screen_id}.png/json
        - Postgres uses ON CONFLICT(screen_id) DO UPDATE
    """
    logger.info("run_id=%s stage=ingest status=started limit=%s", run_id, limit)

    s3 = get_s3_client()
    ensure_bucket_exists(s3)

    ds = load_dataset(
        "rootsautomation/RICO-Screen2Words",
        split="train",
        streaming=True,
        trust_remote_code=True,
    )

    rows_in = 0
    rows_out = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            for row in itertools.islice(ds, limit):
                rows_in += 1

                screen_id = int(row["screenId"])
                png_key = f"screens/{screen_id}.png"
                hierarchy_key = f"screens/{screen_id}.json"

                png_buffer = BytesIO()
                row["image"].save(png_buffer, format="PNG")
                png_bytes = png_buffer.getvalue()

                hierarchy_text = row["view_hierarchy"]
                hierarchy_bytes = hierarchy_text.encode("utf-8")

                s3.put_object(
                    Bucket=MINIO_BUCKET,
                    Key=png_key,
                    Body=png_bytes,
                )

                s3.put_object(
                    Bucket=MINIO_BUCKET,
                    Key=hierarchy_key,
                    Body=hierarchy_bytes,
                )

                source_fingerprint = sha256_bytes(png_bytes)

                cur.execute(
                    """
                    INSERT INTO screens_metadata (
                        screen_id,
                        app_package,
                        category,
                        png_path,
                        hierarchy_json_path,
                        run_id,
                        source_fingerprint,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (screen_id)
                    DO UPDATE SET
                        app_package = EXCLUDED.app_package,
                        category = EXCLUDED.category,
                        png_path = EXCLUDED.png_path,
                        hierarchy_json_path = EXCLUDED.hierarchy_json_path,
                        run_id = EXCLUDED.run_id,
                        source_fingerprint = EXCLUDED.source_fingerprint,
                        updated_at = NOW()
                    """,
                    (
                        screen_id,
                        row["app_package_name"],
                        row["category"],
                        png_key,
                        hierarchy_key,
                        run_id,
                        source_fingerprint,
                    ),
                )

                rows_out += 1

                logger.info(
                    "run_id=%s stage=ingest screen_id=%s png_path=%s hierarchy_path=%s",
                    run_id,
                    screen_id,
                    png_key,
                    hierarchy_key,
                )

        conn.commit()

    logger.info(
        "run_id=%s stage=ingest status=finished rows_in=%s rows_out=%s",
        run_id,
        rows_in,
        rows_out,
    )

    return {
        "stage": "ingest",
        "rows_in": rows_in,
        "rows_out": rows_out,
    }