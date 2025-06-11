class SqsExit(BaseException):
    """
    Base class for SqsConsumer exit exceptions.

    These exceptions should not be handled by client code. As they derive from
    `BaseException`, they won't be caught by `except Exception` blocks.
    """

    def __init__(self, *args):
        super().__init__(*args)


class SqsBackoffExit(SqsExit):
    """
    Stop processing the SQS message due to a temporary error, but keep it in the
    queue to retry it later.

    This exception may be raised when external resources (e.g. the application
    database, other internal microservices or third-party HTTP services) are not
    available at the moment due to network failure, service unavailability,
    usage rate limiting or other temporary factor that might pass in a few
    moments.
    """

    def __init__(self, log_message: str | None = None):
        self.log_message = log_message
        msg = self.compose_message()
        super().__init__(msg)


    def compose_message(self, message_id: str | None = None):
        msg = "Backing off SQS message"

        if message_id: msg += f" {message_id}"
        if self.log_message: msg += f": {self.log_message}"

        return msg


class SqsAbortExit(SqsExit):
    """
    Stop processing the SQS message due to a critical error and delete the
    message from the queue.

    This exception may be raised when a malformed message or an internal code
    failure is detected. The message is removed from the queue, as retrying it
    would only trigger again the same critical error.
    """

    def __init__(self, error_code: str, error_message: str):
        self.error_code = error_code
        self.error_message = error_message

        msg = self.compose_message()
        super().__init__(msg)

    def compose_message(self, message_id: str | None = None):
        if not message_id:
            message_id = ""

        return f"Aborting SQS message{message_id} due to error {self.error_code}: {self.error_message}"
