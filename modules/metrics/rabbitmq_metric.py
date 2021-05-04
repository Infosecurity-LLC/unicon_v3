import json
import logging
from time import sleep
import requests
from modules.metrics import metric_notify_gauge
from retry import retry
from copy import deepcopy

logger = logging.getLogger('unicon.rabbimq_metrics')


class RabbitMQApiClient:
    def __init__(self, settings: dict):
        self.rabbitmq_settings = deepcopy(settings["connection"])
        self.rabbitmq_settings['port'] = settings["api_port"]
        self.session = requests.Session()
        self.session.auth = (self.rabbitmq_settings['username'], self.rabbitmq_settings['password'])
        self.api = f"http://{self.rabbitmq_settings['host']}:{self.rabbitmq_settings['port']}/api/"

    # request attempt within 3 minutes (value of geometric progression sum,
    # # where tries-1 - number of the last term, delay - first term, backoff - denominator)
    @retry(requests.RequestException, tries=10, delay=1, backoff=1.71, logger=logger)
    def requests_get(self, endpoint: str):
        if endpoint[0] == "/":
            endpoint = endpoint[1:]
        try:
            response = self.session.get(self.api + endpoint, timeout=2)
            logger.debug("RMQApi request to %s", response.url)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.warning('Exception raised by rabbitmq api request: %s', str(exc))
            raise exc
        response = json.loads(response.content)
        return response


def check_rmq_metrics(rmq_api_client: RabbitMQApiClient):
    queues = rmq_api_client.requests_get("queues")
    for queue in queues:
        metric_notify_gauge(app_module="rmq", metric_name=f"{queue['name']}_messages-acked",
                            value=queue.get('message_stats', {}).get('ack', 0))
        metric_notify_gauge(app_module="rmq", metric_name=f"{queue['name']}_messages-published",
                            value=queue.get('message_stats', {}).get('publish', 0))  # Messages published recently
        metric_notify_gauge(app_module="rmq", metric_name=f"{queue['name']}_messages-ready",
                            value=queue.get('messages_ready', 0))  # Number of messages ready for delivery
        metric_notify_gauge(app_module="rmq", metric_name=f"{queue['name']}_messages-unacked",
                            value=queue.get('messages_unacknowledged', 0))  # Number of unacknowledged messages


def get_rabbitmq_metric(settings):
    rmq_api_client = RabbitMQApiClient(settings)
    while True:
        check_rmq_metrics(rmq_api_client=rmq_api_client)
        sleep(1)
