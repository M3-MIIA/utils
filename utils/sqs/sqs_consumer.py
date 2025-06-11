from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from pydantic import BaseModel

from .sqs_exceptions import SqsExit


_Message = TypeVar("_Message", bound=BaseModel)


class SqsConsumer(ABC, Generic[_Message]):
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

    async def handle_sqs_exit(self, message: dict,
                              cause: SqsExit | Exception) -> None:
        pass  # Default to no-op if not overridden
