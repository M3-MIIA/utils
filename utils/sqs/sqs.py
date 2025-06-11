# Reference:
# - https://docs.aws.amazon.com/pt_br/lambda/latest/dg/with-sqs.html


import asyncio
import logging
from typing import Iterable, Tuple

from ..async_utils import make_with_semaphore

from .sqs_consumer import _Message, SqsConsumer
from .sqs_exceptions import SqsAbortExit, SqsBackoffExit, SqsExit


async def _handle_sqs_exit(consumer: SqsConsumer[_Message], message: dict,
                           cause: SqsExit):
    try:
        await consumer.handle_sqs_exit(message, cause)
    except Exception | SqsExit as e:
        e.add_note("Exception raised while consumer was handling a previous SQS exit exception")
        raise


async def _process_sqs_message(consumer: SqsConsumer[_Message],
                               record: dict):
    message_id = record["messageId"]
    delete_message = True

    error_message = "Failed parsing SQS message"
    try:
        message = consumer.parse_message(record)
        if custom_message_id := consumer.extract_custom_message_id(message):
            message_id = f"{message_id} ({custom_message_id})"

        error_message = "Failed processing SQS message"
        await consumer.process_job(message)
    except SqsBackoffExit as e:
        delete_message = False
        logging.warning(e.compose_message(message_id))
        await consumer.handle_sqs_exit(record, e)
    except SqsAbortExit as e:
        logging.error(e.compose_message(message_id))
        await consumer.handle_sqs_exit(record, e)
    except Exception as e:
        e.add_note(f"{error_message} {message_id}")
        await consumer.handle_sqs_exit(record, e)
        raise
    finally:
        if delete_message:
            pass  # TODO: boto3 delete message


def log_unhandled_exceptions(
        results: Iterable[Tuple[dict, BaseException | None]]
):
    results = filter(lambda r: r[1] is not None, results)
    for message, result in results:
        message_id = message["messageId"]
        msg = f"Unhandled exception raised while processing job {message_id}"
        logging.error(msg, exc_info=result)


async def process_sqs_messages(consumer: SqsConsumer[_Message], event: dict):
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
    log_unhandled_exceptions(zip(records, results))
