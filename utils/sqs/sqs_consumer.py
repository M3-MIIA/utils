from abc import ABC, abstractmethod
from os import environ
from typing import Generic, TypeVar

from pydantic import BaseModel

from .sqs_exceptions import SqsAbortExit, SqsBackoffExit, SqsExit


_Message = TypeVar("_Message", bound=BaseModel)


class SqsConsumer(ABC, Generic[_Message]):
    """
    Specify a consumer for messages from an SQS queue.

    A consumer must implement the `parse_message()` method. It may also override
    the default implementation of other methods (especially
    `handle_sqs_error()`) as needed.

    # Recommended structure
    The recommended way to specify a consumer is:
    1. Create a function that encapsulates only the business logic, without
       platform specifics (e.g.: database access). Receive all input as
       function parameters and return all outputs as function returned values.
    2. Create a class deriving from `SqsConsumer` and override the methods
       needed to parse the queue message, fetch any input data needed from data
       sources like databases and convert them to the business logic function
       input format (and the other way round to store the function output into
       the matching data sinks).
    3. Create the SQS queue lambda handler that calls the `process_sqs_messages()`
       function using the `SqsConsumer` created in the previous step.

    E.g.:
    ```
    # ❶ Business logic function
    # --------------------------------------------------------------------------
    @dataclass
    class AssessEssayInput:
        essay_theme: str
        reference_texts: str
        essay_text: str

    @dataclasss
    class AssessEssayOutput:
        score: decimal
        remarks: str

    def assess_essay(essay: AssessEssayInput) -> AssessEssayOutput:
        […]


    # ❷ SQS queue message consumer
    # --------------------------------------------------------------------------
    class AssessEssaySqsMessage(pydantic.BaseModel):
        id: int

    async def fetch_essay_from_db(id: int) -> AssessEssayInput:
        […]

    async def store_assessment_in_db(id: int,
                                     assessment: AssessEssayOutput) -> None:
        […]

    async def store_assessment_error_in_db(id: int, error_code,
                                           error_message) -> None:
        […]

    class AssessEssaySqsConsumer(SqsConsumer[AssessEssaySqsMessage]):
        @override
        async def process_job(self, message: AssessEssaySqsMessage) -> None:
            input_data = fetch_essay_from_db(message.id)
            output_data = assess_essay(input_data)  # ⬅️ Call the business logic function
            store_assessment_in_db(output_data)

        @override
        async def handle_sqs_error(self, raw_message: dict,
                                  message: AssessEssaySqsMessage,
                                  cause: SqsExit | Exception,
                                  error_code: str, error_message: str) -> None:
            store_assessment_error_in_db(message.id, error_code, error_message)


    # ❸ SQS queue lambda handler
    # --------------------------------------------------------------------------
    def lambda_handler(event: dict, context):
        loop = asyncio.get_event_loop()
        sqs_consumer = AssessEssaySqsConsumer()
        loop.run_until_complete(process_sqs_messages(sqs_consumer, event))
    ```
    Note in the example above how the `assess_essay()` business logic function
    is isolated from platform details of data sources and sinks. This allows
    this same function to be called in various contexts, like:
    1. In unit tests with fixed input data
    2. In local runs with data read from and written to files in the filesystem
    3. Queue event handlers with data stored in databases
    """

    def __init__(self, queue_url: str | None = None) -> None:
        super().__init__()
        if queue_url is None:
            queue_url = environ.get("MIIA_SQS_QUEUE_URL")

        assert queue_url, "Missing SQS queue URL"
        self.queue_url = queue_url


    @property
    def max_parallel_tasks(self) -> int | None:
        return None  # Use default from `make_with_semaphore()` (see `process_sqs_messages()`)


    def parse_message(self, message: dict) -> _Message:
        raw_body = message["body"]
        body = _Message.model_validate_json(raw_body)
        return body


    def extract_custom_message_id(self, message: _Message) -> str | None:
        return getattr(message, "id", None)


    @abstractmethod
    async def process_job(self, message: _Message) -> None:
        ...


    async def handle_sqs_exit(self, raw_message: dict, message: _Message | None,
                              cause: SqsExit | Exception) -> None:
        if (
            message is None  # Has no message content to match with the error
            or isinstance(cause, SqsBackoffExit)  # Will try again, it's not the
                                                  # final state of the message
        ):
            return  # Don't treat as error

        if isinstance(cause, SqsAbortExit):
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
