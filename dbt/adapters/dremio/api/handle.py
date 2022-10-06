from dbt.adapters.dremio.api.cursor import DremioCursor

from dbt.adapters.dremio.api.basic import login

#from dbt.logger import GLOBAL_LOGGER as logger
from dbt.events import AdapterLogger
logger = AdapterLogger("dremio")

class DremioHandle:
    def __init__(self, host, port, UID, PWD):
        self._host = host
        self._port = port
        self._UID = UID
        self._PWD = PWD
        self._cursor = None
        self.closed = False
    
    def __build_base_url(self):
        return "http://{host}:{port}".format(host=self._host, port=self._port)

    def cursor(self):
        if self.closed:
            raise Exception("HandleClosed")
        if self._cursor is None or self._cursor.closed:
            base_url = self.__build_base_url()
            token = login(base_url, self._UID, self._PWD)
            self._cursor = DremioCursor(self._host, self._port, token)
        return self._cursor

    def close(self):
        if self.closed:
            raise Exception("HandleClosed")
        self.closed = True


    def rollback(self):
        #todo
        logger.debug("Handle Rollback is not ready yet.")