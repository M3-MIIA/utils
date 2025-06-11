from asyncio import Semaphore
from collections.abc import Coroutine
from contextvars import ContextVar
import logging
from os import environ


class AsyncTaskLogFilter(logging.Filter):
    """
    Add a task ID prefix in log messages for async contexts.

    E.g.:
    ```
    # ❶ Install the log filter in the logger:
    logging.getLogger().addFilter(AsyncTaskLogFilter())

    async def process_job(job):
        job_id = job["id"]  # E.g.: "1234"
        AsyncTaskLogFilter.task_id.set(essay_id)  # ❷ Set the current context ID

        # ❸ The ID will be added to all log messages from this async context:
        logging.info("Job started")  # → "[1234] Job started"
        …  # Processa o trabalho
        logging.info(f"Job complete (took {…}s)") # → "[1234] Job complete (took 3s)"
    ```
    """

    task_id = ContextVar("task_id", default="")

    def filter(self, record):
        task_id_ = self.__class__.task_id.get()
        if task_id_:
            record.msg = f"[{task_id_}] {record.msg}"
        return True


def _get_default_max_parallel_tasks():
    return int(environ.get('MIIA_MAX_PARALLEL_TASKS', 3))

def make_semaphore(max_tasks: int | None = None):
    """
    Return a `Semaphore <https://docs.python.org/3/library/asyncio-sync.html#asyncio.Semaphore>`_
    object with `max_tasks` count.

    If `max_tasks` count is not given, use:
    - The `MIIA_MAX_PARALLEL_TASKS` environment variable (if set)
    - A hardcoded default value (as last resort)

    :returns: The semaphore object and the maximum parallel tasks count
    """

    if max_tasks is None:
        max_tasks = _get_default_max_parallel_tasks()

    return Semaphore(max_tasks), max_tasks


def make_with_semaphore(max_tasks: int | None = None):
    """
    Return a wrapper function for running coroutines inside a semaphore to limit
    the maximum number of concurrent tasks.

    The `max_tasks` behaviour is the same as in `make_semaphore()` function.

    E.g.:
    ```
    async def do_work(value):
        ...

    wth_semaphore, max_tasks = make_with_semaphore(3)

    tasks = [ with_semaphore(do_work(v)) for v in range(10) ]

    logging.info(f"Running {len(tasks)} tasks with at most {max_tasks} in parallel")
    results = asyncio.gather(*tasks)
    ```
    """

    semaphore, max_tasks = make_semaphore(max_tasks)

    async def with_semaphore[Y, S, R](f: Coroutine[Y, S, R]):
        async with semaphore:
            return await f

    return with_semaphore, max_tasks
