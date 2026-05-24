from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from src.audit import run_duplicate_audit
from src.embed_image import embed_images
from src.embed_text import embed_text
from src.eval import run_eval
from src.extract import extract_structured_data
from src.ingest import ingest_screens
from src.metrics import collect_metrics
from src.runs import create_pipeline_run, finish_pipeline_run
from src.slack import post_slack_message


@dag(
    dag_id="rico_multimodal_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["rico", "multimodal", "homework"],
)
def rico_multimodal_pipeline():
    start = EmptyOperator(task_id="start")

    @task
    def init_run(**context):
        conf = context["dag_run"].conf or {}
        limit = int(conf.get("LIMIT", 5))

        run_id = create_pipeline_run(
            dag_run_id=context["dag_run"].run_id,
            limit_param=limit,
        )

        post_slack_message(
            f"RICO pipeline started\nrun_id={run_id}\nLIMIT={limit}\ntrigger={context['dag_run'].run_type}"
        )

        return {
            "run_id": run_id,
            "limit": limit,
        }

    @task
    def ingest_task(run_info: dict):
        return ingest_screens(
            run_id=run_info["run_id"],
            limit=run_info["limit"],
        )

    @task
    def parse_task(run_info: dict):
        # Parsing is currently used inside embed_text and extract.
        # This task exists because the homework requires parse as a visible DAG node.
        return {
            "stage": "parse",
            "rows_in": run_info["limit"],
            "rows_out": run_info["limit"],
        }

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
        # In this implementation, stages write directly to destination tables.
        # This task is kept as a visible production boundary required by the homework.
        return {
            "stage": "load",
            "status": "completed_by_upserts",
            "run_id": run_info["run_id"],
        }

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
    def metrics_task(run_info: dict):
        return collect_metrics(run_info["run_id"])

    @task
    def finish_task(run_info: dict):
        metrics = collect_metrics(run_info["run_id"])

        finish_pipeline_run(run_info["run_id"], "succeeded")

        post_slack_message(
            "RICO pipeline finished\n"
            f"run_id={run_info['run_id']}\n"
            "status=succeeded\n"
            f"summary={metrics['summary']}"
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