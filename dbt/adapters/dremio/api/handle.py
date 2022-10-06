from dbt.adapters.dremio.api.cursor import DremioCursor

class DremioHandle:
    def __init__(self, cursor:DremioCursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def close(self):
        pass