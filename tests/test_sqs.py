import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning,
                        module="botocore", message=r".*datetime\.utcnow\(\).*")

from pydantic import BaseModel
import pytest

from utils.exceptions import ServiceUnavailable

from utils.sqs import \
    process_sqs_messages, SqsAbortMessage, SqsBackoffMessage, SqsConsumer, \
    SqsDuplicateMessage, SqsExit

import utils.sqs.sqs as sqs_internal


class DummyMessage(BaseModel):
    id: int

DUMMY_MESSAGE_ID = 1248
DUMMY_MESSAGE = {
    "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
    "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
    "body": f'{{ "id": {DUMMY_MESSAGE_ID} }}'
}

DUMMY_QUEUE_EVENT = {
    "Records": [ DUMMY_MESSAGE ]
}

class DummyConsumer(SqsConsumer[DummyMessage]):
    def __init__(self, queue_url: str | None = "dummy_queue",
                 actions: dict | None = None):
        super().__init__(DummyMessage, queue_url)
        self.actions = actions if actions is not None else {}
        self.calls = {}

    async def process_job(self, message: DummyMessage) -> None:
        if action := self.actions.get(message.id):
            action()

    async def handle_sqs_exit(self, raw_message: dict,
                              message: DummyMessage | None,
                              cause: SqsExit | Exception) -> None:
        message_id = message.id if message else None
        calls = self.calls.setdefault(message_id, {})
        calls = calls.setdefault('handle_sqs_exit', [])
        calls.append(type(cause))
        await super().handle_sqs_exit(raw_message, message, cause)

    async def handle_sqs_error(self, raw_message: dict, message: DummyMessage,
                               cause: SqsExit | Exception,
                               error_code, error_message) -> None:
        message_id = message.id if message else None
        calls = self.calls.setdefault(message_id, {})
        calls = calls.setdefault('handle_sqs_error', [])
        calls.append((type(cause), error_code, error_message))
        await super().handle_sqs_error(raw_message, message, cause, error_code,
                                       error_message)


@pytest.fixture
def deleted_sqs_message_calls(monkeypatch):
    class MockSqsClient:
        def __init__(self):
            self.calls = []

        def delete_message(self, **kwargs):
            self.calls.append(kwargs)

    mock_client = MockSqsClient()
    monkeypatch.setattr(sqs_internal.boto3, "client", lambda _: mock_client)

    yield mock_client.calls

    sqs_internal._sqs_client = None  # Remove mock from cache


def test_missing_queue_url(monkeypatch):
    monkeypatch.delenv("MIIA_SQS_QUEUE_URL", raising=False)
    with pytest.raises(AssertionError):
        DummyConsumer(queue_url=None)


async def test_queue_url_from_env_var(deleted_sqs_message_calls, monkeypatch):
    queue_url = "LorenIpsunDolor"
    monkeypatch.setenv("MIIA_SQS_QUEUE_URL", queue_url)

    consumer = DummyConsumer(queue_url=None)
    await process_sqs_messages(consumer, DUMMY_QUEUE_EVENT)

    assert deleted_sqs_message_calls == [{
        "QueueUrl": queue_url,
        "ReceiptHandle": DUMMY_MESSAGE["receiptHandle"]
    }]

    assert consumer.calls == {}, \
           "No consumer error function should have been called"


async def test_invalid_message(deleted_sqs_message_calls):
    consumer = DummyConsumer()
    await process_sqs_messages(consumer, {
        "Records": [
            { **DUMMY_MESSAGE, "body": '{ "missing": "id field" }' }
        ]
    })

    assert deleted_sqs_message_calls == [{
        "QueueUrl": consumer.queue_url,
        "ReceiptHandle": DUMMY_MESSAGE["receiptHandle"]
    }]

    calls = consumer.calls[None]
    assert calls.get("handle_sqs_exit") == [ SqsAbortMessage ], \
        "handle_sqs_exit was not called as expected"

    assert calls.get("handle_sqs_error") == None, \
        "handle_sqs_error shouldn't have been called"


EXCEPTION_TEST_CASES = {
    "abort": {
        "exception": SqsAbortMessage("loren", "ipsum"),
        "should_delete_from_queue": True,
        "error_code": "loren", "error_message": "ipsum"
    },
    "backoff": {
        "exception": SqsBackoffMessage(),
        "should_delete_from_queue": False
    },
    "duplicate": {
        "exception": SqsDuplicateMessage(),
        "should_delete_from_queue": True
    },
    "exception": {
        "exception": RuntimeError(),
        "should_delete_from_queue": True,
        "error_code": "internal_server_error",
        "error_message": "Internal server error"
    }
}

@pytest.mark.parametrize('case', EXCEPTION_TEST_CASES)
async def test_abort_message(case, deleted_sqs_message_calls):
    case = EXCEPTION_TEST_CASES[case]

    exception = case["exception"]
    def action():
        raise exception

    message_id = DUMMY_MESSAGE_ID
    consumer = DummyConsumer(actions={
        message_id: action
    })

    if isinstance(exception, SqsExit):
        await process_sqs_messages(consumer, DUMMY_QUEUE_EVENT)
    else:
        with pytest.raises(ExceptionGroup):
            await process_sqs_messages(consumer, DUMMY_QUEUE_EVENT)

    if case["should_delete_from_queue"]:
        assert deleted_sqs_message_calls == [{
            "QueueUrl": consumer.queue_url,
            "ReceiptHandle": DUMMY_MESSAGE["receiptHandle"]
        }]
    else:
        assert deleted_sqs_message_calls == [], \
            "Message should have been kept in the queue"

    calls = consumer.calls[message_id]
    assert calls.get("handle_sqs_exit") == [ type(exception) ], \
        "handle_sqs_exit was not called as expected"

    if error_code := case.get("error_code"):
        assert calls.get("handle_sqs_error") == [
            (type(exception), error_code, case["error_message"])
        ], "handle_sqs_error was not called as expected"
    else:
        assert calls.get("handle_sqs_error") == None, \
            "handle_sqs_error shouldn't have been called"


async def test_translate_exception(deleted_sqs_message_calls):
    def action():
        raise ServiceUnavailable()

    message_id = DUMMY_MESSAGE_ID
    consumer = DummyConsumer(actions={
        message_id: action
    })

    await process_sqs_messages(consumer, DUMMY_QUEUE_EVENT)

    assert deleted_sqs_message_calls == [], \
        "Message should have been kept in the queue"

    calls = consumer.calls[message_id]
    assert calls.get("handle_sqs_exit") == [ SqsBackoffMessage ], \
        "handle_sqs_exit was not called as expected"

    assert calls.get("handle_sqs_error") == None, \
        "handle_sqs_error shouldn't have been called"


if __name__ == "__main__":
    import sys
    status = pytest.main()
    sys.exit(status)
