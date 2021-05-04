from .mssql_connector.connector import MSSQLConnector
from .kav_connector.connector import KAVConnector
from .pt_connector.connector import PTConnector

connectors_map = {
    "kav_connector": KAVConnector,
    "pt_connector": PTConnector
}
