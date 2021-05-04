from retry import retry
from typing import Dict, NoReturn
from raven.conf import setup_logging
from raven.handlers.logging import SentryHandler
import logging
import redis

logger = logging.getLogger('unicon')


def prepare_logging(settings: Dict) -> NoReturn:
    """
    Prepares logger for application

    :param settings: Application settings (i.e. from data/settings.yaml)
    :return:
    """
    logger.setLevel(settings['logging']['basic_level'])
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(settings['logging']['term_level'])
    stream_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)-10s - [in %(pathname)s:%(lineno)d]: - %(message)s')
    )
    logger.addHandler(stream_handler)
    if settings.get('sentry_url'):
        sentry_handler = SentryHandler(settings['sentry_url'])
        sentry_handler.setLevel(settings['logging']['sentry_level'])
        setup_logging(sentry_handler)
        logger.addHandler(sentry_handler)


def init_redis_connection_pool(redis_config: Dict) -> redis.Redis:
    """
    Создаём пул подключений к redis
    :param redis_config:
    :return:
    """
    from redis import Redis
    from redis import ConnectionPool
    pool = ConnectionPool(**redis_config)
    connection = Redis(connection_pool=pool)
    return connection


@retry(Exception, tries=5, delay=15, logger=logger)
def health_check_rmq(rmq_config: Dict) -> NoReturn:
    """
    Проверяем, прогрузился ли RMQ при старте сборки
    docker-compose считает сервис запущенным по факту запуска сервиса,
        не проверяя его окончательную загрузку (depends_on в этому случае не спасает),
        поэтому зависимый сервис может начать стучаться в непрогруженный RMQ
    :param rmq_config:
    :return:
    """
    from modules.buffering.amqp import create_rmq_connection
    from pika.exceptions import AMQPConnectionError
    try:
        create_rmq_connection(rmq_config)
    except AMQPConnectionError as err:
        raise AMQPConnectionError("Fail connect to RMQ (%s)" % err)
    logger.debug("RMQ OK")


@retry(Exception, tries=5, delay=15, logger=logger)
def health_check_redis(redis_config: Dict):
    import environment as env
    """
    Проверяем, прогрузился ли redis при старте сборки, аналогично health_check_rmq
    За одно поднимаем пул подключений для сервиса
    :param redis_config: 
    :return: 
    """
    env.redis = init_redis_connection_pool(redis_config)
    env.redis.get("test")
    logger.debug("REDIS OK")
