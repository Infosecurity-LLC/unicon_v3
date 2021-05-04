import hashlib
import orjson
import logging
import socket
from typing import Dict
from dateutil.parser import parse
from retry import retry
from modules.nxlog_client import NXLogClient
import environment as env
from modules.models import NxlogMessage
from modules.metrics import metric_notify_counter
from datetime import datetime
import decimal
import re

logger = logging.getLogger('unicon')


class ErrorUnknownConnectorId(Exception):
    pass


def md5_from_raw(data):
    hash_t = hashlib.md5()
    hash_t.update(str(data).encode('utf8'))
    return hash_t.hexdigest()


def get_nx_attributes(connector_id) -> Dict:
    """
    Получение атрибутов, необходимых для отправки сообщени я в nxlog
    имеем какой-то конфиг по умолчанию и дополняем/обновляем его данными для конкретного коннектора
    :param connector_id: id конкретного коннектора
    :return:
    """

    def serialize_nx_config(config: dict) -> Dict:

        """
        Сериализация конфига для nxlog, приведение ключей к нижнему регистру
        *Только для первого уровня вложенности
        :param config:
        :return:
        """
        serializable_config = {}
        for k, v in config.items():
            serializable_config[re.sub(r'(?<!^)(?=[A-Z])', '_', k).lower()] = v
        return serializable_config

    _nx_attributes = env.nxlog_config['nx_attributes']['default']
    if connector_id not in env.nxlog_config['nx_attributes']:
        logger.error("No exist connector_id [%s] in nx settings" % connector_id)
        raise ErrorUnknownConnectorId
    if not env.nxlog_config['nx_attributes'][connector_id]:
        return serialize_nx_config(_nx_attributes)
    return serialize_nx_config({**_nx_attributes, **env.nxlog_config['nx_attributes'][connector_id]})


@retry(Exception, tries=3, delay=2, backoff=2, logger=logger)
def nxlog_callback(ch, method, properties, body):
    """
    Callback on consumed message

    :param ch: consuming channel
    :param method:
    :param properties:
    :param body: message from queue
    :return:
    """

    def nx_formatter(event: dict) -> Dict:
        """
        Форматирование nx'овой обвязки
            connector_id и dt - обязательные поля в событии
        :param event:
        :return:
        """

        def cast(message: dict):
            """приведение типов"""
            for k, v in message.items():
                if isinstance(v, datetime):
                    message[k] = message[k].isoformat()
                if isinstance(v, decimal.Decimal):
                    message[k] = int(message[k])
                try:
                    message[k] = int(message[k])
                except (ValueError, TypeError):
                    pass
                if k in ['username']:
                    message[k] = str(message[k])
            return message

        nx_attributes = get_nx_attributes(event['connector_id'])
        f_message = NxlogMessage(**nx_attributes)
        f_message.hostname = socket.gethostname()
        event_time = parse(event['dt'])
        f_message.event_time = event_time
        f_message.detection_time = event_time
        f_message.raw = event
        f_message.md5 = md5_from_raw(event)
        return cast(f_message.to_dict())

    rmq_message = orjson.loads(body)
    logger.debug("Received message from queue: %s", rmq_message)
    metric_notify_counter(app_module=rmq_message['connector_id'], metric_name="stream-of-events")

    # if event is already exists in redis, there's no need in sending to nxlog
    rmq_message_id = f"{rmq_message['connector_id']}_{rmq_message['id']}_{md5_from_raw(rmq_message)}"

    if env.redis.exists(rmq_message_id):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.debug(f"{rmq_message['id']} already exist")
        return

    nx_message = nx_formatter(rmq_message)
    logger.debug("Try to send event to NXLog [%s] %s", nx_message['raw']['connector_id'], nx_message['raw'])

    if not env.nxlog_client:
        env.nxlog_client = NXLogClient(**env.nxlog_config['nx_collector'])
    if env.nxlog_client.send_event(nx_message):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        metric_notify_counter(app_module=rmq_message['connector_id'],
                              metric_name=f"sent_messages_{nx_message['DevType']}")

    # put into redis after successful sending
    env.redis.set(rmq_message_id, body, ex=1209600)  # срок хранения данных в базе 14 дней
    metric_notify_counter(app_module=rmq_message['connector_id'], metric_name="received-events")

    return
