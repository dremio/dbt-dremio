from dbt.adapters.dremio.api.cursor import DremioCursor
from dbt.adapters.dremio.api.parameters import Parameters

from dbt.adapters.dremio.api.basic import login

from dbt.events import AdapterLogger
logger = AdapterLogger("dremio")

class DremioHandle:
    def __init__(self, parameters: Parameters):
        self._parameters = parameters
        self._cursor = None
        self.closed = False

    def cursor(self):
        if self.closed:
            raise Exception("HandleClosed")
        if self._cursor is None or self._cursor.closed:
            self._parameters = login(self._parameters)
            self._cursor = DremioCursor(self._parameters)
        return self._cursor

    def close(self):
        if self.closed:
            raise Exception("HandleClosed")
        self.closed = True

    def rollback(self):
        #todo
        raise Exception("Handle rollback not implemented.")