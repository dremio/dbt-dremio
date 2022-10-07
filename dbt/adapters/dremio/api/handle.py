from dbt.adapters.dremio.api.cursor import DremioCursor

from dbt.adapters.dremio.api.basic import login

from dbt.events import AdapterLogger
logger = AdapterLogger("dremio")

class DremioHandle:
    def __init__(self, base_url, UID, PWD):
        self._base_url = base_url
        self._UID = UID
        self._PWD = PWD
        self._cursor = None
        self.closed = False

    def cursor(self):
        if self.closed:
            raise Exception("HandleClosed")
        if self._cursor is None or self._cursor.closed:
            token = login(self._base_url, self._UID, self._PWD)
            self._cursor = DremioCursor(self._base_url, token)
        return self._cursor

    def close(self):
        if self.closed:
            raise Exception("HandleClosed")
        self.closed = True

    def rollback(self):
        #todo
        raise Exception("Handle rollback not implemented.")