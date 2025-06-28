from abc import ABC, abstractmethod
import logging
from os import environ
from typing import Generic, TypeVar

from pydantic import BaseModel, ValidationError

from .sqs_exceptions import SqsAbortMessage, SqsExit


_Message = TypeVar("_Message", bound=BaseModel)


class SqsConsumer(ABC, Generic[_Message]):
    """
    Specify a consumer for messages from an SQS queue.

    A consumer must implement the `parse_message()` method. It may also override
    the default implementation of other methods (especially
    `handle_sqs_error()`) as needed.

    The SQS queue URL may be passes in the constructor. If not, it is fetched
    from the `MIIA_SQS_QUEUE_URL` environment variable.

    The recommended way to specify a consumer is:
    1. Create a function that encapsulates only the business logic.
    2. Create a class deriving from `SqsConsumer` and override the
       `process_job()` method to call the business logic.
    3. Create the SQS queue lambda handler that calls the `process_sqs_messages()`
       function using the `SqsConsumer` created in the previous step.

    E.g.:
    ```
    # ❶ Business logic function
    def assess_essay(essay: AssessEssayInput) -> AssessEssayOutput:
        […]


    # ❷ SQS queue message consumer
    class AssessEssaySqsMessage(pydantic.BaseModel):
        id: int

    class AssessEssaySqsConsumer(SqsConsumer[AssessEssaySqsMessage]):
        @override
        async def process_job(self, message: AssessEssaySqsMessage) -> None:
            input_data = fetch_essay_from_db(message.id)
            output_data = assess_essay(input_data)  # Business logic
            store_assessment_in_db(output_data)

        @override
        async def handle_sqs_error(self, raw_message: dict,
                                  message: AssessEssaySqsMessage,
                                  cause: SqsExit | Exception,
                                  error_code: str, error_message: str) -> None:
            store_assessment_error_in_db(message.id, error_code, error_message)


    # ❸ SQS queue lambda handler
    def lambda_handler(event: dict, context):
        loop = asyncio.get_event_loop()
        sqs_consumer = AssessEssaySqsConsumer()  # Instantiate consumer
        loop.run_until_complete(
            process_sqs_messages(sqs_consumer, event)  # Process messages
        )
    ```

    More info: [Notion](https://www.notion.so/2119e5d01b7f801aadacf30c91eaaad8)
    """

    def __init__(self, message_type: type[_Message],
                 queue_url: str | None = None) -> None:
        self.message_type = message_type

        if queue_url is None:
            queue_url = environ.get("MIIA_SQS_QUEUE_URL")

        assert queue_url, "Missing SQS queue URL"
        self.queue_url = queue_url


    @property
    def max_parallel_tasks(self) -> int | None:
        return None  # Use default from `make_with_semaphore()` (see `process_sqs_messages()`)


    def parse_message(self, message: dict) -> _Message:
        """
        Parse the received SQS message body.

        The boby is parsed using `model_validate_json()` method from the message
        model.
        """

        try:
            raw_body = message["body"]
            body = self.message_type.model_validate_json(raw_body)
            return body
        except ValidationError as e:
            logging.error(f"Failed parsing message body: {e}")
            e = SqsAbortMessage("internal_server_error",
                                "Internal server error")
            e.add_note("Failed parsing SQS message")
            raise e


    def extract_custom_message_id(self, message: _Message) -> str | None:
        return getattr(message, "id", None)


    @abstractmethod
    async def process_job(self, message: _Message) -> None:
        ...


    async def handle_sqs_exit(self, raw_message: dict, message: _Message | None,
                              cause: SqsExit | Exception) -> None:
        if (
            message is None  # Has no message content to match with the error
            or (isinstance(cause, SqsExit) and not cause._is_error)
        ):
            return  # Don't treat as error

        if isinstance(cause, SqsAbortMessage):
            error_code = cause.error_code
            error_message = cause.error_message
        else:
            error_code = "internal_server_error"
            error_message = "Internal server error"

        await self.handle_sqs_error(raw_message, message, cause, error_code,
                                    error_message)


    async def handle_sqs_error(self, raw_message: dict, message: _Message,
                               cause: SqsExit | Exception,
                               error_code: str, error_message: str) -> None:
        pass  # Default to no-op if not overridden
