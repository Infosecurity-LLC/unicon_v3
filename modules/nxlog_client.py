import socket
import logging
from retry import retry
import json

logger = logging.getLogger("unicon.nxlog_client")


class NXLogClientError(Exception):
    pass


class NXLogClient:
    def __init__(self, host, port):
        self.__setting = (host, port)
        self.sock = None
        self.connect()

    @retry(Exception, tries=10, delay=2, backoff=2, logger=logger)
    def connect(self):
        logger.info("Create NXLog connection")
        try:
            self.sock = socket.create_connection(self.__setting)
        except ConnectionRefusedError:
            raise NXLogClientError('Нет доступа до сервера nxlog %s:%s' % self.__setting)

    @retry(Exception, tries=3, delay=2, backoff=2, logger=logger)
    def send_event(self, message: dict):
        try:
            jmessage = json.dumps(message)
            logger.info("Sending message to NXLog {}".format(jmessage))
            self.sock.send("{}\n".format(str(jmessage)).encode("utf-8"))
        except BrokenPipeError:
            logger.error('Сессия к NXLog оборвалась. Событие не было отправлено {}'.format(message))
            self.connect()
            raise Exception
        except AttributeError:
            logger.error('Событие не было отправлено {}'.format(message))
            return False
        except Exception as err:
            logger.error(err)
            return False
        return True

    def close(self):
        try:
            self.sock.close()
        except AttributeError:
            logger.error('Нельзя закрыть несуществующую сессию')
        except Exception as err:
            logger.error(err)
