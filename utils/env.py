import inspect
import logging
from os import environ, getcwd, chdir
from os.path import dirname


def init_logger():
    """
    Should be called in the global scope of the main file.

    See also: `init_env()`, which calls `init_logger()` after reading the `.env`
    file
    """
    log_level = environ.get('MIIA_LOG_LEVEL', 'INFO').upper()
    logging.getLogger().setLevel(log_level)


def init_env():
    """
    Read `.env` files and initialize the environment and logging.
    """

    from dotenv import find_dotenv, load_dotenv

    orig_cwd = None

    try:
        # Search .env file from caller location by temporarily switching cwd:
        frame = inspect.stack()[1]
        if frame.filename:
            orig_cwd = getcwd()
            new_cwd = dirname(frame.filename)
            chdir(new_cwd)

        path = find_dotenv(usecwd=True)
        load_dotenv(path)
    finally:
        if orig_cwd:  # Restore original cwd state
            chdir(orig_cwd)

    init_logger()
