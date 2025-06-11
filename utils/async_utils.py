from asyncio import Semaphore
from collections.abc import Coroutine
from os import environ


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
