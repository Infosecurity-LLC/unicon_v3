import logging
from modules.connectors.mssql_connector.connector import MSSQLConnector

logger = logging.getLogger("kav_connector")


class KAVConnector(MSSQLConnector):
    default_module_config = 'modules/connectors/kav_connector/default_config.yaml'
    ...
