import logging
from os import environ


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

    from dotenv import load_dotenv
    load_dotenv()
    init_logger()
