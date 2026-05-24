from src.runs import create_pipeline_run, finish_pipeline_run
from src.ingest import ingest_screens

run_id = create_pipeline_run(
    dag_run_id="manual-test-ingest",
    limit_param=2,
)

try:
    result = ingest_screens(run_id=run_id, limit=2)
    finish_pipeline_run(run_id, "manual-test-succeeded")
    print(result)
    print("run_id:", run_id)
except Exception:
    finish_pipeline_run(run_id, "manual-test-failed")
    raise