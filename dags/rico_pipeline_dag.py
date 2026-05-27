from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from src.audit import run_duplicate_audit
from src.embed_image import embed_images
from src.embed_text import embed_text
from src.eval import run_eval
from src.extract import extract_structured_data
from src.ingest import ingest_screens
from src.metrics import collect_metrics
from src.runs import create_pipeline_run, finish_pipeline_run
from src.slack import post_slack_message
from src.metrics import collect_metrics, save_metric
from src.db import get_conn
import time
import logging

logger = logging.getLogger(__name__)

def mark_pipeline_failed(context):
    """
    Airflow task failure callback.

    If any task fails after init_run, mark the pipeline run as failed
    and send a best-effort Slack notification.
    """
    ti = context.get("ti")
    task_instance = context.get("task_instance")
    exception = context.get("exception")

    task_id = task_instance.task_id if task_instance else "unknown"

    # audit_task handles paused-by-audit itself
    if task_id == "audit_task":
        return

    run_info = None
    if ti:
        run_info = ti.xcom_pull(task_ids="init_run")

    if not run_info or "run_id" not in run_info:
        logger.warning("Could not mark pipeline failed: run_id unavailable")
        return

    run_id = run_info["run_id"]

    try:
        finish_pipeline_run(run_id, "failed")
    except Exception:
        logger.exception("Failed to update pipeline_runs for failed run")

    try:
        post_slack_message(
            "RICO pipeline failed\n"
            f"run_id={run_id}\n"
            f"task={task_id}\n"
            f"error={exception}"
        )
    except Exception:
        logger.exception("Failed to post Slack failure notification")

@dag(
    dag_id="rico_multimodal_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["rico", "multimodal", "homework"],
    params={
        "LIMIT": Param(
            5,
            type="integer",
            minimum=1,
            description="Number of RICO screens to process",
        )
    },
    default_args={
        "on_failure_callback": mark_pipeline_failed,
    },
)
def rico_multimodal_pipeline():
    start = EmptyOperator(task_id="start")

    @task
    def init_run():
        context = get_current_context()

        limit = int(context["params"].get("LIMIT", 5))
        dag_run_id = context["dag_run"].run_id

        run_id = create_pipeline_run(
            dag_run_id=dag_run_id,
            limit_param=limit,
        )

        run_info = {
            "run_id": str(run_id),
            "limit": limit,
            "dag_run_id": dag_run_id,
        }

        post_slack_message(
            "RICO pipeline started\n"
            f"run_id={run_info['run_id']}\n"
            f"limit={run_info['limit']}\n"
            "trigger=manual_or_scheduled"
        )

        return run_info

    @task
    def ingest_task(run_info: dict):
        return ingest_screens(
            run_id=run_info["run_id"],
            limit=run_info["limit"],
        )

    @task
    def parse_task(run_info: dict):
        start = time.perf_counter()
        result = {
            "stage": "parse",
            "rows_in": run_info["limit"],
            "rows_out": run_info["limit"],
        }
        save_metric(
            run_info["run_id"],
            "task_duration_seconds.parse",
            round(time.perf_counter() - start, 3),
        )
        save_metric(run_info["run_id"], "task_rows_in.parse", run_info["limit"])
        save_metric(run_info["run_id"], "task_rows_out.parse", run_info["limit"])
        return result

    @task
    def embed_image_task(run_info: dict):
        return embed_images(
            run_id=run_info["run_id"],
            limit=run_info["limit"],
        )

    @task
    def embed_text_task(run_info: dict):
        return embed_text(
            run_id=run_info["run_id"],
            limit=run_info["limit"],
        )

    @task
    def extract_task(run_info: dict):
        return extract_structured_data(
            run_id=run_info["run_id"],
            limit=run_info["limit"],
        )

    @task
    def load_task(run_info: dict):
        start = time.perf_counter()
        result = {
            "stage": "load",
            "status": "completed_by_upserts",
            "run_id": run_info["run_id"],
        }
        save_metric(
            run_info["run_id"],
            "task_duration_seconds.load",
            round(time.perf_counter() - start, 3),
        )
        return result

    @task
    def audit_task(run_info: dict):
        try:
            return run_duplicate_audit(run_info["run_id"])
        except Exception as exc:
            post_slack_message(
                f"RICO pipeline audit failed\nrun_id={run_info['run_id']}\nerror={exc}"
            )
            finish_pipeline_run(run_info["run_id"], "paused-by-audit")
            raise

    @task
    def eval_task(run_info: dict):
        return run_eval(run_info["run_id"])

    @task
    def finish_task(run_info: dict):
        finish_pipeline_run(run_info["run_id"], "succeeded")

        metrics = collect_metrics(run_info["run_id"])

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXTRACT(EPOCH FROM (ended_at - started_at))
                    FROM pipeline_runs
                    WHERE run_id = %s
                    """,
                    (run_info["run_id"],),
                )
                duration = round(float(cur.fetchone()[0]), 3)

        post_slack_message(
            "RICO pipeline finished\n"
            f"run_id={run_info['run_id']}\n"
            "status=succeeded\n"
            f"duration={duration}s"
        )

        return metrics

    run_info = init_run()

    ingest_result = ingest_task(run_info)
    parse_result = parse_task(run_info)

    image_result = embed_image_task(run_info)
    text_result = embed_text_task(run_info)
    extract_result = extract_task(run_info)

    load_result = load_task(run_info)
    audit_result = audit_task(run_info)
    eval_result = eval_task(run_info)
    finish_result = finish_task(run_info)

    start >> run_info >> ingest_result >> parse_result

    parse_result >> [image_result, text_result, extract_result]
    [image_result, text_result, extract_result] >> load_result
    load_result >> audit_result >> eval_result >> finish_result


rico_multimodal_pipeline()