import logging
import environment as env
from modules.buffering import source
from modules.metrics import metric_notify_counter

logger = logging.getLogger("unicon")


class ConnectorError(Exception):
    pass


class ErrorNotExitsRequiredParameter(Exception):
    pass


class BaseConnector:
    """Base connector class"""
    default_module_config = ''
    required_parameters = frozenset([])

    def __init__(self):
        self.inited = False
        self.rmq = source.RMQProducer(settings=env.settings['rabbitmq']['connection'],
                                      exchange_label=env.target_config['exchange_label'])

    def __init_metrics(self, app_module):
        if not self.inited:
            metric_notify_counter(app_module, "run-counter", is_init=True)
            metric_notify_counter(app_module, "run-fail-counter", is_init=True)
            metric_notify_counter(app_module, "run-success-counter", is_init=True)

    def send_write_to_rmq(self, write: dict):
        logger.info("Save to rabbitmq %s", write)
        self.rmq.send_write_to_rmq(item=write)

    def connector(self, settings, **kwargs):
        # Your connector
        pass

    def start(self, settings, **kwargs):
        self.__init_metrics(settings['connector_id'])
        if self.default_module_config:
            default_settings = env.get_settings(self.default_module_config)
            settings = {**default_settings, **settings}
        for key in self.required_parameters:
            if key not in settings:
                metric_notify_counter(settings['connector_id'], "run-fail-counter")
                logger.error("Not exist required parameter %s", key)
                raise ErrorNotExitsRequiredParameter
        try:
            self.connector(settings, **kwargs)
        except Exception as err:
            metric_notify_counter(settings['connector_id'], "run-fail-counter")
            raise ConnectorError(err)
        else:
            metric_notify_counter(settings['connector_id'], "run-success-counter")
        finally:
            metric_notify_counter(settings['connector_id'], "run-counter")
