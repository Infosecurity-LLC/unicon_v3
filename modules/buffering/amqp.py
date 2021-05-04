import logging
from typing import Dict
from pika import exceptions

import pika

logger = logging.getLogger('unicon')


class RabbitMQConnectionError(Exception):
    pass


def create_rmq_connection(settings: Dict) -> pika.adapters.blocking_connection.BlockingConnection:
    """
    Creates blocking connection to RabbitMQ

    :param settings: RabbitMQ settings
    :return: pika.adapters.blocking_connection.BlockingConnection
    """
    settings = settings.copy()
    username = settings.pop('username', None)
    password = settings.pop('password', None)
    parameters = pika.ConnectionParameters(
        credentials=pika.credentials.PlainCredentials(username, password), **settings
    )
    try:
        connection = pika.BlockingConnection(parameters)
    except exceptions.IncompatibleProtocolError as err:
        raise RabbitMQConnectionError("Error connection %s", err)
    logger.debug("Create rmq connection channel with parameters %s", str(parameters))
    return connection


def prepare_producer_channel(connection: pika.adapters.blocking_connection.BlockingConnection,
                             exchange_name: str) -> pika.adapters.blocking_connection.BlockingChannel:
    """
    Создание канала от producer до RabbitMQ exchange

    Create channel for producer to RabbitMQ exchange with prepared connection

    :param connection: Подготовленное соединение RabbitMQ
    :param exchange_name:
    :return:
    """
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)
    logger.debug("Declare rmq exchange %s for producer channel %s", exchange_name, str(channel))
    return channel


def prepare_consumer_channel(settings: Dict, exchange_name: str,
                             queue_name: str, routing_key: str) -> pika.adapters.blocking_connection.BlockingChannel:
    """
    Create channel from consumer to queue and bind queue to exchange with routing key

    :param settings:
    :param exchange_name:
    :param queue_name:
    :param routing_key:
    :return:
    """
    connection = create_rmq_connection(settings)
    channel = connection.channel()
    logger.debug("Channal %s", str(channel))

    channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)
    logger.debug("Declare rmq exchange %s for consumer channel", exchange_name)

    channel.queue_declare(queue=queue_name, durable=True)
    logger.debug("Declare rmq queue %s for consumer channel", queue_name)

    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)
    logger.info("Bind queue %s to exchange %s with routing key %s", exchange_name, queue_name, routing_key)

    channel.basic_qos(prefetch_count=1)
    return channel
