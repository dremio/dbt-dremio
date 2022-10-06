from email.mime import base
from warnings import catch_warnings
import agate
from typing import Tuple, Union
from typing import Optional, Union, Any
from dataclasses import dataclass
from contextlib import contextmanager
from textwrap import indent

import pyodbc
import time
import json

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.dremio.relation import DremioRelation
from dbt.contracts.connection import AdapterResponse

# poc hack
from dbt.adapters.dremio.api.basic import login
from dbt.adapters.dremio.api.endpoints import delete_catalog, sql_endpoint, job_status, \
    set_catalog, catalog_item
from dbt.adapters.dremio.api.error import DremioAlreadyExistsException

#from dbt.logger import GLOBAL_LOGGER as logger
from dbt.events import AdapterLogger
logger = AdapterLogger("dremio")


@dataclass
class DremioCredentials(Credentials):
    driver: str
    host: str
    UID: str
    PWD: str
    environment: Optional[str]
    database: Optional[str]
    schema: Optional[str]
    datalake: Optional[str]
    root_path: Optional[str]
    port: Optional[int] = 31010  # for sql endpoint, not rest
    additional_parameters: Optional[str] = None
    # poc hack
    token: Optional[str] = None
    rest_api_port: Optional[int] = 9047
    #

    _ALIASES = {
        'user': 'UID',
        'username': 'UID',
        'pass': 'PWD',
        'password': 'PWD',
        'server': 'host',
        'track': 'environment',
        'space': 'database',
        'folder': 'schema',
        'materialization_database': 'datalake',
        'materialization_schema': 'root_path'
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
        return 'driver', 'host', 'port', 'UID', 'database', 'schema', 'additional_parameters', 'datalake', 'root_path', 'environment'

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

        except pyodbc.DatabaseError as e:
            logger.debug('Database error: {}'.format(str(e)))

            try:
                # attempt to release the connection
                self.release()
            except pyodbc.Error:
                logger.debug("Failed to release connection!")
                pass

            raise dbt.exceptions.DatabaseException(str(e).strip()) from e

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
        # breakpoint()

        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = connection.credentials

        try:
            con_str = ["ConnectionType=Direct",
                       "AuthenticationType=Plain", "QueryTimeout=600"]
            con_str.append(f"Driver={{{credentials.driver}}}")
            con_str.append(f"HOST={credentials.host}")
            con_str.append(f"PORT={credentials.port}")
            con_str.append(f"UID={credentials.UID}")
            con_str.append(f"PWD={credentials.PWD}")
            if credentials.additional_parameters:
                con_str.append(f"{credentials.additional_parameters}")
            con_str_concat = ';'.join(con_str)
            logger.debug(f'Using connection string: {con_str_concat}')

            handle = pyodbc.connect(con_str_concat, autocommit=True)

            connection.state = 'open'
            connection.handle = handle
            logger.debug(f'Connected to db: {credentials.database}')

        except pyodbc.Error as e:
            logger.debug(f"Could not connect to db: {e}")

            connection.handle = None
            connection.state = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        # REST API hack
        base_url = cls._build_base_url(credentials)
        token = login(base_url, credentials.UID, credentials.PWD)
        connection.credentials.token = token
        ##

        return connection

    @classmethod
    def is_cancelable(cls) -> bool:
        return False

    def cancel(self, connection):
        pass

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
                cursor.execute(sql, bindings)

            # poc hack
            token = connection.credentials.token
            base_url = self._build_base_url(connection.credentials)
            json_payload = sql_endpoint(
                token, base_url, sql, context=None, ssl_verify=True)

            job_id = json_payload["id"]
            json_payload = job_status(token, base_url, job_id, ssl_verify=True)

            # Next steps:
            # keep checking job staus until status is one of COMPLETE, CANCELLED, FAILED
            # then call endpoints.job_results -> payload is schema and rows (unlikey to use offset as there shouldn't be too many rows)
            # mapr job results to cursor
            ##

            logger.debug("SQL status: {} in {:0.2f} seconds".format(
                         self.get_response(cursor), (time.time() - pre)))

            return connection, cursor

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_response(cls, cursor: pyodbc.Cursor) -> AdapterResponse:
        rows = cursor.rowcount
        message = 'OK' if rows == -1 else str(rows)
        return AdapterResponse(
            _message=message,
            rows_affected=rows
        )

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

    @classmethod
    def _build_base_url(cls, credentials: DremioCredentials) -> str:
        return "http://{host}:{port}".format(host=credentials.host, port=credentials.rest_api_port)

    def drop_catalog(self, database, schema):
        connection = self.get_thread_connection()
        credentials = connection.credentials
        base_url = self._build_base_url(credentials)

        if (database == '@' + credentials.UID):
            logger.debug("Skipping drop schema")
            return

        path = [database]
        folders = schema.split(".")
        path.extend(folders)

        catalog_info = catalog_item(
            credentials.token, base_url, None, path, False)
        delete_catalog(credentials.token, base_url,
                       catalog_info.id, catalog_info.tag, False)

    def create_catalog(self, database, schema):
        connection = self.get_thread_connection()
        credentials = connection.credentials

        base_url = self._build_base_url(credentials)
        token = login(base_url, credentials.UID, credentials.PWD)
        connection.credentials.token = token

        path = [database]
        folders = schema.split(".")
        path.extend(folders)

        # if default space then create the folder within the space only
        if (database == '@' + credentials.UID):
            logger.debug("Database is default: creating folder only")
            folder_json = self._make_new_folder_json(path)
            try:
                set_catalog(credentials.token, base_url, folder_json, False)
            except DremioAlreadyExistsException:
                logger.debug("Folder already exists. Returning.")

            """credentials.database = credentials.datalake
            credentials.schema = credentials.root_path """
            return

        space_json = self._make_new_space_json(database)

        try:
            set_catalog(credentials.token, base_url, space_json, False)
        except DremioAlreadyExistsException:
            logger.debug("Database already exists. Skipping creation.")
        """ try:
                set_catalog(credentials.token, base_url, folder_json, False)
            except DremioAlreadyExistsException:
                logger.debug("Folder already exists. Returning.")
        except:
            pass
        """

    def _make_new_space_json(self, name) -> json:
        python_dict = {
            "entityType": "space",
            "name": name
        }
        return json.dumps(python_dict)

    def _make_new_folder_json(self, path) -> json:
        python_dict = {
            "entityType": "folder",
            "path": path

        }
        return json.dumps(python_dict)
