import logging
from dataclasses import dataclass
from typing import NoReturn

from pika.adapters.blocking_connection import BlockingChannel

from .callbacks.nxlog import nxlog_callback


logger = logging.getLogger('unicon')


@dataclass
class TargetTask:
    queue_label: str
    connector_id: str


def target_consuming_executor(task: TargetTask, channel: BlockingChannel) -> NoReturn:
    """
    Starts endless consuming messages from RabbitMQ queue

    :param task: Parameters for producer (routing key and queue label)
    :param channel: Prepared blocking channel for consuming
    :return:
    """
    channel.basic_consume(queue=task.queue_label, on_message_callback=nxlog_callback)
    channel.start_consuming()
    logger.warning("Should appear only on exception. !!!")
