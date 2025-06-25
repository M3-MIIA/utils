from abc import ABC, abstractmethod
import logging
from typing import override


class SqsExit(ABC, BaseException):
    """
    Base class for SqsConsumer exit exceptions.

    These exceptions should not be handled by client code. As they derive from
    `BaseException`, they won't be caught by `except Exception` blocks.
    """

    _is_error = True
    _delete_message = True

    def __init__(self, *args):
        super().__init__(*args)

    @abstractmethod
    def log(self, message_id: str | None) -> None:
        ...

    def _do_compose_message(self, preamble: str,
                            log_message: str | None = None,
                            message_id: str | None = None):
        msg = preamble

        if message_id: msg += f" {message_id}"
        if log_message: msg += f": {log_message}"

        return msg


class SqsDuplicateMessage(SqsExit):
    """
    Stop processing an SQS message that was detected to be a duplicate that has
    already been processed previously.

    This exception may be raised when the message processing code checks that
    there is already an output registered in a database associated to the
    current message from a previous run in order to avoid reprocessing the same
    data twice or triggering duplicate downstream actions.

    The message will be removed from the queue and no further action will be
    taken.
    """

    _is_error = False

    def __init__(self, log_message: str | None = None):
        self.log_message = log_message
        msg = self._compose_message()
        super().__init__(msg)

    @override
    def log(self, message_id: str | None):
        msg = self._compose_message(message_id)
        logging.warning(msg)

    def _compose_message(self, message_id: str | None = None):
        return self._do_compose_message("Skipping duplicate SQS message",
                                        message_id, self.log_message)


class SqsBackoffMessage(SqsExit):
    """
    Stop processing the SQS message due to a temporary error, but keep it in the
    queue to retry it later.

    This exception may be raised when external resources (e.g. the application
    database, other internal microservices or third-party HTTP services) are not
    available at the moment due to network failure, service unavailability,
    usage rate limiting or other temporary factor that might pass in a few
    moments.
    """

    _is_error = False
    _delete_message = False

    def __init__(self, log_message: str | None = None):
        self.log_message = log_message
        msg = self._compose_message()
        super().__init__(msg)

    @override
    def log(self, message_id: str | None):
        msg = self._compose_message(message_id)
        logging.warning(msg)

    def _compose_message(self, message_id: str | None = None):
        return self._do_compose_message("Backing off SQS message",
                                        message_id, self.log_message)


class SqsAbortMessage(SqsExit):
    """
    Stop processing the SQS message due to a critical unrecoverable error that
    blocks its processing.

    This exception may be raised when a malformed message or an internal code
    failure is detected. The message is removed from the queue, as retrying it
    would only trigger again the same critical error.
    """

    def __init__(self, error_code: str, error_message: str,
                 extra_fields: dict | None = None):
        self.error_code = error_code
        self.error_message = error_message
        self.extra_fields = extra_fields

        msg = self._compose_message()
        super().__init__(msg)

    @override
    def log(self, message_id: str | None):
        msg = self._compose_message(message_id)
        logging.error(msg)

    def _compose_message(self, message_id: str | None = None):
        if not message_id:
            message_id = ""

        return f"Aborting SQS message {message_id} due to error {self.error_code}: {self.error_message}"
