import asyncio
import logging
from typing import Iterable, Tuple

import boto3

from ..async_utils import AsyncTaskLogFilter, make_with_semaphore
from ..exceptions import MiiaError, ServiceUnavailable

from .sqs_consumer import _Message, SqsConsumer
from .sqs_exceptions import SqsAbortMessage, SqsBackoffMessage, SqsExit


logging.getLogger().addFilter(AsyncTaskLogFilter())


DUMMY_SQS_QUEUE_URL = 'dummy-sqs-queue'


def _translate_exception(exception: Exception) -> SqsExit | None:
    try:
        raise exception
    except ServiceUnavailable:
        return SqsBackoffMessage(str(exception))
    except MiiaError as e:
        return SqsAbortMessage(e.error_code, e.message)
    except Exception:
        return None


async def _handle_sqs_exit(consumer: SqsConsumer[_Message], raw_message: dict,
                           message: _Message | None,
                           cause: SqsExit | Exception):
    try:
        await consumer.handle_sqs_exit(raw_message, message, cause)
    except (Exception, SqsExit) as e:
        e.add_note("Exception raised while consumer was handling a previous SQS exit exception")
        raise


def _should_delete_message(exception: Exception | SqsExit | None):
    if exception is None: return True  # Message processed successfully
    elif not isinstance(exception, SqsExit): return True  # Unhandled exception
    else: return exception._delete_message


_sqs_client = None

def _delete_sqs_message(consumer: SqsConsumer, message: dict, message_id: str):
    global _sqs_client

    if consumer.queue_url == DUMMY_SQS_QUEUE_URL:
        logging.warning(f"Using dummy SQS queue, skipping message {message_id} delete")
        return

    try:
        if _sqs_client is None:
            _sqs_client = boto3.client("sqs")

        _sqs_client.delete_message(
            QueueUrl=consumer.queue_url,
            ReceiptHandle=message["receiptHandle"]
        )
    except Exception as e:
        _sqs_client = None  # Force reconnection in next attempt
        e.add_note(f"Failed deleting message {message_id} from SQS queue")
        raise


async def _process_sqs_message(consumer: SqsConsumer[_Message],
                               record: dict):
    message_id = record["messageId"]

    message = None
    exception = None

    AsyncTaskLogFilter.task_id.set(message_id)

    error_message = "Failed parsing SQS message"
    try:
        message = consumer.parse_message(record)
        if custom_message_id := consumer.extract_custom_message_id(message):
            message_id = f"{message_id} ({custom_message_id})"
            AsyncTaskLogFilter.task_id.set(message_id)

        error_message = "Failed processing SQS message"
        await consumer.process_job(message)
    except SqsExit as e:
        exception = e
        e.log(message_id)
    except Exception as e:
        if translated := _translate_exception(e):
            logging.info(f"Unhandled {type(e)} exception translated to {type(translated)}")
            exception = translated
            exception.log(message_id)
        else:
            exception = e
            e.add_note(f"{error_message} {message_id}")
            raise  # The exception will be logged by log_unhandled_exceptions()
    finally:
        if exception:
            await _handle_sqs_exit(consumer, record, message, exception)

        if _should_delete_message(exception):
            _delete_sqs_message(consumer, record, message_id)


def _log_unhandled_exceptions(
        results: Iterable[Tuple[dict, BaseException | None]]
):
    exceptions = [ r for r in results if isinstance(r[1], Exception) ]
    if not exceptions:
        return

    for message, exception in exceptions:
        message_id = message["messageId"]
        msg = f"Unhandled exception raised while processing job {message_id}"
        logging.error(msg, exc_info=exception)

    exceptions = [ r[1] for r in exceptions if isinstance(r[1], Exception) ]
    raise ExceptionGroup("Unhandled errors in SQS processing",
                         exceptions)


async def process_sqs_messages(consumer: SqsConsumer[_Message], event: dict):
    """
    Process received SQS queue messages from lambda integration `event`.

    See the SqsConsumer class for more info on how the messages are processed.

    If any unhandled exception (aside from `SqsExit` ones) is raised, only the
    message in which the exception occurred is aborted: Other received messages
    continue being processed concurrently. After all messages have been
    processed, if any unhandled exception occurred, an ExceptionGroup exception
    holding all unhandled exceptions is raised at the end of the processing.

    More info: https://docs.aws.amazon.com/pt_br/lambda/latest/dg/with-sqs.html
    """

    records = event.get("Records")
    assert records != None, "Malformed event dict (missing \"Records\" entry)"

    max_parallel_tasks = consumer.max_parallel_tasks
    with_semaphore, max_parallel_tasks = make_with_semaphore(max_parallel_tasks)
    logging.info(f"Processing {len(records)} jobs in {max_parallel_tasks} threads")

    tasks = (
        with_semaphore(_process_sqs_message(consumer, r))
        for r in records
    )

    results = await asyncio.gather(*tasks, return_exceptions=True)
    _log_unhandled_exceptions(zip(records, results))
