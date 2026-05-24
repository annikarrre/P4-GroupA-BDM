import functools
import time

from src.metrics import save_metric


def timed_stage(stage_name: str):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(run_id: str, *args, **kwargs):
            start = time.perf_counter()

            try:
                return func(run_id, *args, **kwargs)
            finally:
                duration = time.perf_counter() - start
                save_metric(
                    run_id,
                    f"task_duration_seconds.{stage_name}",
                    round(duration, 3),
                )

        return wrapper

    return decorator