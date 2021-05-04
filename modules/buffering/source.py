import json
import logging
from typing import Dict

from pika import BasicProperties
from pika.exceptions import AMQPError
from retry import retry

from modules.buffering.amqp import create_rmq_connection, prepare_producer_channel
from modules.models import UniconEvent

logger = logging.getLogger('unicon')


class RMQProducer:
    def __init__(self, settings: Dict, exchange_label: str):
        """
        :param settings: settings to create pika connection and channel
        :param exchange_label:
        """
        self.settings = settings
        self.exchange_label = exchange_label
        self.connection = None
        self.producer_channel = None
        self.create_connection()

    @retry(AMQPError, tries=2, delay=2, backoff=2, logger=logger)
    def create_connection(self):
        self.connection = create_rmq_connection(settings=self.settings)
        self.producer_channel = prepare_producer_channel(self.connection, self.exchange_label)
        logger.debug("Create RabbitMQ producer channel: %s", str(self.producer_channel))

    @retry(AMQPError, tries=5, delay=2, backoff=2, logger=logger)
    # def send_write_to_rmq(item: UniconEvent, exchange_label: str, rabbit_settings: Dict) -> bool:
    def send_write_to_rmq(self, item: dict) -> bool:
        """
        Publish provided item to RabbitMQ exchange

        :param item: write to send to NXLog
        :return:
        """

        self.producer_channel.basic_publish(
            exchange=self.exchange_label,
            # routing_key=item.SensorName,
            routing_key=item.get('connector_id'),
            # TODO: make UniconEvent model JSON serializable
            body=json.dumps(item),
            # make message persistent
            properties=BasicProperties(delivery_mode=2)
        )

        return True

    def __del__(self):
        logger.debug("Close RabbitMQ channel %s", str(self.producer_channel))
        self.producer_channel.close()
        self.connection.close()
