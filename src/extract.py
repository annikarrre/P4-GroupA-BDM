import json
import logging

import boto3
import requests

from src.config import (
    LLM_MODEL,
    MINIO_BUCKET,
    MINIO_KEY,
    MINIO_SECRET,
    MINIO_URL,
    OLLAMA_URL,
    PROMPT_VERSION,
)
from src.db import get_conn
from src.parse import parse_hierarchy, text_representation
from src.timing import timed_stage
from src.utils import sha256_text

logger = logging.getLogger(__name__)


PROMPT_V1 = """\
You are a UI structure extractor for Android app screenshots.

Given the visible text from one screen's view hierarchy, return a single
JSON object with these fields:

- "title": a short string naming the screen (e.g. "Login", "Settings",
  "Search results"). Empty string if unclear.
- "elements": a list of {"type": string, "text": string} objects, one
  per salient interactive or informational element you can identify.
- "confidence": a number in [0.0, 1.0] expressing how confident you are
  in the extraction.

Visible text:
{hierarchy_text}

Respond with valid JSON only — no commentary, no Markdown fences.
"""


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_URL,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
    )


def extract_one(text_rep: str) -> dict:
    prompt = PROMPT_V1.replace("{hierarchy_text}", text_rep)

    response = requests.post(
        f"{OLLAMA_URL}/api/generate",
        json={
            "model": LLM_MODEL,
            "prompt": prompt,
            "stream": False,
            "format": "json",
        },
        timeout=120,
    )

    response.raise_for_status()
    raw = response.json()["response"]
    return json.loads(raw)

@timed_stage("extract")
def extract_structured_data(run_id: str, limit: int) -> dict:
    logger.info("run_id=%s stage=extract status=started", run_id)

    s3 = get_s3_client()

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

    rows_out = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            for screen_id, hierarchy_json_path in rows:
                raw = (
                    s3.get_object(Bucket=MINIO_BUCKET, Key=hierarchy_json_path)["Body"]
                    .read()
                    .decode("utf-8")
                )

                text_rep = text_representation(parse_hierarchy(raw))
                source_fingerprint = sha256_text(text_rep)

                try:
                    payload = extract_one(text_rep)
                    confidence = float(payload.get("confidence", 0.0))
                    body = {k: v for k, v in payload.items() if k != "confidence"}

                    cur.execute(
                        """
                        UPDATE screens_metadata
                        SET extraction_payload = %s::jsonb,
                            prompt_version = %s,
                            confidence = %s,
                            source_fingerprint = %s,
                            updated_at = NOW()
                        WHERE screen_id = %s
                          AND run_id = %s
                        """,
                        (
                            json.dumps(body),
                            PROMPT_VERSION,
                            confidence,
                            source_fingerprint,
                            screen_id,
                            run_id,
                        ),
                    )

                    if confidence < 0.5:
                        cur.execute(
                            """
                            INSERT INTO screens_review_queue (
                                screen_id,
                                run_id,
                                source_fingerprint,
                                reason
                            )
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                            """,
                            (
                                screen_id,
                                run_id,
                                source_fingerprint,
                                "low_confidence_extraction",
                            ),
                        )

                    rows_out += 1

                except Exception as exc:
                    logger.exception(
                        "run_id=%s stage=extract screen_id=%s status=failed",
                        run_id,
                        screen_id,
                    )

                    cur.execute(
                        """
                        INSERT INTO screens_review_queue (
                            screen_id,
                            run_id,
                            source_fingerprint,
                            reason
                        )
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            screen_id,
                            run_id,
                            source_fingerprint,
                            f"extraction_failed: {exc}",
                        ),
                    )

        conn.commit()

    logger.info(
        "run_id=%s stage=extract status=finished rows_in=%s rows_out=%s",
        run_id,
        len(rows),
        rows_out,
    )

    return {
        "stage": "extract",
        "rows_in": len(rows),
        "rows_out": rows_out,
    }