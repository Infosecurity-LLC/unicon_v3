import logging
import socutils
from datetime import datetime
from modules.connectors.base_connector import BaseConnector
from modules.connectors.mssql_connector.mssql_collector import MsSQLCollector

logger = logging.getLogger("unicon")


class ErrorNotEnoughInstructions(Exception):
    pass


class MSSQLConnector(BaseConnector):
    required_parameters = frozenset([
        'config',
        'organization',
        'connector_id',
        'query',
        'start_time',
        'end_time'
    ])

    def connector(self, settings, **kwargs):
        self.settings = settings
        if 'config' not in self.settings or not self.settings['config']:
            logger.error("No instructions for connecting to db (config)")
            raise ErrorNotEnoughInstructions()
        config = socutils.get_settings(self.settings['config'])
        collector = MsSQLCollector(config)
        if isinstance(self.settings['start_time'], datetime):
            self.settings['start_time'] = self.settings['start_time'].strftime("%Y-%m-%d %H:%m:%S")
        if isinstance(self.settings['end_time'], datetime):
            self.settings['end_time'] = self.settings['end_time'].strftime("%Y-%m-%d %H:%m:%S")

        writes = collector.select_writes(query=self.settings['query'],
                                         start_time=self.settings['start_time'],
                                         end_time=self.settings['end_time'])
        for write in writes:
            write['connector_id'] = self.settings['connector_id']
            write['organization'] = self.settings['organization']

            self.send_write_to_rmq(write)
