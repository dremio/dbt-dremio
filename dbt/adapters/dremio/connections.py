from contextlib import contextmanager

from typing import List
from dbt.adapters.dremio.api.cursor import DremioCursor
from dbt.adapters.dremio.api.handle import DremioHandle

import time

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.dremio.relation import DremioRelation
from dbt.contracts.connection import AdapterResponse

#from dbt.logger import GLOBAL_LOGGER as logger
from dbt.events import AdapterLogger
logger = AdapterLogger("dremio")

from dataclasses import dataclass
from typing import Optional, Union, Any

from typing import Tuple, Union
import agate

@dataclass
class DremioCredentials(Credentials):
    driver: str
    host: str
    environment: Optional[str]
    database: Optional[str]
    schema: Optional[str]
    datalake: Optional[str]
    root_path: Optional[str]
    UID: Optional[str] = None
    PWD: Optional[str] = None
    port: Optional[int] = 32010
    use_ssl: Optional[bool] = True
    additional_parameters: Optional[str] = None
    pat: Optional[str] = None

    _ALIASES = {
        'user': 'UID'
        , 'username': 'UID'
        , 'pass': 'PWD'
        , 'password': 'PWD'
        , 'server': 'host'
        , 'track': 'environment'
        , 'space': 'database'
        , 'folder': 'schema'
        , 'materialization_database' : 'datalake'
        , 'materialization_schema' : 'root_path'
    }

    @property
    def type(self):
        return 'dremio'

    @property
    def unique_field(self):
        return self.host

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'
        # raise NotImplementedError
        return 'driver', 'host', 'port', 'UID', 'database', 'schema', 'additional_parameters', 'datalake', 'root_path', 'environment', 'use_ssl'

    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(data)
        if 'database' not in data:
            data['database'] = None
        if 'schema' not in data:
            data['schema'] = None
        if 'datalake' not in data:
            data['datalake'] = None
        if 'root_path' not in data:
            data['root_path'] = None
        if 'environment' not in data:
            data['environment'] = None
        if 'pat' not in data:
            data['pat'] = None
        return data

    def __post_init__(self):
        if self.database is None:
              self.database = '@' + self.UID
        if self.schema is None:
              self.schema = DremioRelation.no_schema
        if self.datalake is None:
              self.datalake = '$scratch'
        if self.root_path is None:
              self.root_path = DremioRelation.no_schema

class DremioConnectionManager(SQLConnectionManager):
    TYPE = 'dremio'

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield   
        except Exception as e:
            logger.debug(f"Error running SQL: {sql}")
            logger.debug("Rolling back transaction.")
            self.release()
            if isinstance(e, dbt.exceptions.RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt.exceptions.RuntimeException(e)

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection
        credentials = connection.credentials
        try:
            handle = DremioHandle(credentials.host, credentials.port, credentials.UID, credentials.PWD)
            _ = handle.cursor()
            connection.state = 'open'
            connection.handle = handle
            logger.debug(f'Connected to db: {credentials.database}')
        except Exception as e:
            logger.debug(f"Could not connect to db: {e}")
            connection.handle = None
            connection.state = 'fail'
            raise dbt.exceptions.FailedToConnectException(str(e))
        return connection

    @classmethod
    def is_cancelable(cls) -> bool:
        return True

    def cancel(self, connection):
        return connection.handle.cursor.job_cancel()

    def commit(self, *args, **kwargs):
        pass

    def rollback(self, *args, **kwargs):
        pass

    def add_begin_query(self):
        # return self.add_query('BEGIN TRANSACTION', auto_begin=False)
        pass

    def add_commit_query(self):
        # return self.add_query('COMMIT TRANSACTION', auto_begin=False)
        pass

    def add_query(self, sql, auto_begin=True, bindings=None,
                  abridge_sql_log=False):
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()
        logger.debug('Using {} connection "{}".'
                     .format(self.TYPE, connection.name))
        with self.exception_handler(sql):
            if abridge_sql_log:
                logger.debug('On {}: {}....'.format(
                    connection.name, sql[0:512]))
            else:
                logger.debug('On {}: {}'.format(connection.name, sql))
            pre = time.time()
            cursor = connection.handle.cursor()
            # pyodbc does not handle a None type binding!
            if bindings is None:
                cursor.execute(sql)
            else:
                logger.debug(f"Bindings: {bindings}")
                cursor.execute(sql, bindings)
            logger.debug("SQL status: {} in {:0.2f} seconds".format(self.get_response(cursor), (time.time() - pre)))
            return connection, cursor

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_response(cls, cursor: DremioCursor) -> AdapterResponse:
        rows = cursor.rowcount
        message = 'OK' if rows == -1 else str(rows)
        return AdapterResponse(
            _message=message,
            rows_affected=rows
        )
    
    @classmethod
    def get_result_from_cursor(cls, cursor:DremioCursor) -> agate.Table:
        json_payload = cursor.job_results()
        json_rows = json_payload["rows"]
        return agate.Table.from_object(json_rows)

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[AdapterResponse, agate.Table]:
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        fetch = True
        if fetch:
            table = self.get_result_from_cursor(cursor)
        else:
            table = dbt.clients.agate_helper.empty_table()
        cursor.close()
        return response, table

