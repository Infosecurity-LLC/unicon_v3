import logging
import decimal
import pymssql
from datetime import datetime

logger = logging.getLogger("unicon")


class DbaseException(Exception):
    pass


class MsSQLSelector:
    def __init__(self, db_settings):
        self.cursor = None
        self.device = db_settings['host']
        try:
            self.connection = pymssql.connect(server=db_settings['host'],
                                              port=db_settings['port'],
                                              user=db_settings['user'],
                                              password=db_settings['password'],
                                              database=db_settings['database'])
        except pymssql.OperationalError as err:
            raise DbaseException(f'[{self.device}] Dbase server connection failed {err}')

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def raw_query(self, query):
        self.cursor = self.connection.cursor()
        try:
            self.cursor.execute(query)
        except pymssql.ProgrammingError:
            raise DbaseException(
                f'[{self.device}] SQL ProgrammingError at dbase.select function. Error in sql select: {query}')
        return self.cursor


class MsSQLCollector:
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.result_count = None

    def select_writes(self, query: str = None,
                      start_time: str = None,
                      end_time: str = None):
        """
        Генератор выборки событий из базы.
        Возвращаем итератор с сырым событием
        :param query:
        :param start_time:
        :param end_time:
        :return:
        """
        eg = MsSQLSelector(self.db_config)

        cur = eg.raw_query(query.format(start_time=start_time, end_time=end_time))
        result = cur.fetchall()
        logger.info("Got %s records from mssql %s", len(result), self.db_config['host'])
        self.result_count = len(result)
        select_keys = list(field[0] for field in cur.description)
        for val in result:
            write = dict(zip(select_keys, val))
            for k, v in write.items():
                if isinstance(v, datetime):
                    write[k] = write[k].isoformat()
                if isinstance(v, decimal.Decimal):
                    write[k] = int(write[k])
            yield write
