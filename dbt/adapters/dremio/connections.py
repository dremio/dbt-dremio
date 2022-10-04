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
from dbt.adapters.dremio.api.endpoints import sql_endpoint, job_status, job_results

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
    UID: str
    PWD: str
    environment: Optional[str]
    database: Optional[str]
    schema: Optional[str]
    datalake: Optional[str]
    root_path: Optional[str]
    port: Optional[int] = 31010 # for sql endpoint, not rest
    additional_parameters: Optional[str] = None
    # poc hack
    token: Optional[str] = None
    rest_api_port: Optional[int] = 9047
    ##

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
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = connection.credentials

        try:
            con_str = ["ConnectionType=Direct", "AuthenticationType=Plain", "QueryTimeout=600"]
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
        base_url = _build_base_url(credentials)
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
            base_url = _build_base_url(connection.credentials)
            json_payload = sql_endpoint(token, base_url, sql, context=None, ssl_verify=True)
            
            job_id = json_payload["id"]
            
            #logger.debug("SQL status: {} in {:0.2f} seconds".format(self.get_response(cursor), (time.time() - pre)))

            return connection, cursor, job_id

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_response(cls, connection, cursor: pyodbc.Cursor, job_id) -> AdapterResponse:
        #rows = cursor.rowcount
        #message = 'OK' if rows == -1 else str(rows)
        #return AdapterResponse(
        #    _message=message,
        #    rows_affected=rows
        #)
        return api_get_response(connection, job_id)

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[AdapterResponse, agate.Table]:
        sql = self._add_query_comment(sql)
        connection, cursor, job_id = self.add_query(sql, auto_begin)
        response = self.get_response(connection, cursor, job_id)
        fetch = True
        if fetch:
            table = self.get_result_from_cursor(cursor)
            api_table = get_result_from_api(connection, job_id)
            if table is not None:
                with open('dremio.connections.get_result_from_cursor.txt', 'a') as fp:
                    fp.write(f"Job ID : {job_id}\n")
                    table.print_table(max_rows=None, max_columns=None, output=fp, max_column_width=500)
                    fp.write("\n")
            if api_table is not None:
                with open('dremio.connections.get_result_from_api.txt', 'a') as fp:
                    fp.write(f"Job ID : {job_id}\n")
                    api_table.print_table(max_rows=None, max_columns=None, output=fp, max_column_width=500)
                    fp.write("\n")
        else:
            table = dbt.clients.agate_helper.empty_table()
        cursor.close()
        return response, table


def _build_base_url(credentials : DremioCredentials) -> str:
    return "http://{host}:{port}".format(host=credentials.host, port=credentials.rest_api_port)

def api_get_response(connection, job_id) -> AdapterResponse:
    ## keep checking job status until status is one of COMPLETE, CANCELLED or FAILED
    ## map job results to AdapterResponse
    token = connection.credentials.token
    base_url = _build_base_url(connection.credentials)
    last_job_state = ""
    job_status_response = job_status(token, base_url, job_id, ssl_verify=True)
    job_status_state = job_status_response["jobState"]

    while True:
        if job_status_state != last_job_state:
            logger.debug(f"Job State = {job_status_state}")
        if job_status_state == "COMPLETED":
            message = job_status_state
            break
        elif job_status_state == "CANCELLED" or job_status_state == "FAILED":
            message = job_status_state + ": " + job_status_response["errorMessage"]
            break
        last_job_state = job_status_state
        job_status_response = job_status(token, base_url, job_id, ssl_verify=True)
        job_status_state = job_status_response["jobState"]

    #this is done as job status does not return a rowCount if there are no rows affected (even in completed job_state)
    #pyodbc Cursor documentation states "[rowCount] is -1 if no SQL has been executed or if the number of rows is unknown.
    # Note that it is not uncommon for databases to report -1 immediately after a SQL select statement for performance reasons."
    if "rowCount" not in job_status_response:
        rows = -1
        logger.debug("rowCount does not exist in job_status payload")
    else:
        rows = job_status_response["rowCount"]

    return AdapterResponse(
        _message = message,
        rows_affected = rows
    )

def get_result_from_api(connection, job_id) -> agate.Table:
    
    token = connection.credentials.token
    base_url = _build_base_url(connection.credentials)
    json_payload = job_results(token, base_url, job_id, offset=0, limit=100, ssl_verify=True)["rows"]
    return agate.Table.from_object(json_payload)