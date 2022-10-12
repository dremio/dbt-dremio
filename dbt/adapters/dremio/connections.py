from email.mime import base
from warnings import catch_warnings
import agate
from typing import Tuple, Union
from typing import Optional, Union, Any
from dataclasses import dataclass
from contextlib import contextmanager
from textwrap import indent

from typing import List
from dbt.adapters.dremio.api.cursor import DremioCursor
from dbt.adapters.dremio.api.handle import DremioHandle
from dbt.adapters.dremio.api.parameters import Parameters
from dbt.adapters.dremio.api.authentication import DremioAuthentication

import time
import json

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.dremio.relation import DremioRelation
from dbt.contracts.connection import AdapterResponse

from dbt.adapters.dremio.api.basic import login
from dbt.adapters.dremio.api.endpoints import (
    delete_catalog,
    sql_endpoint,
    job_status,
    create_catalog,
    catalog_item,
)
from dbt.adapters.dremio.api.error import (
    DremioAlreadyExistsException,
    DremioNotFoundException,
)

# from dbt.logger import GLOBAL_LOGGER as logger

from dbt.events import AdapterLogger

logger = AdapterLogger("dremio")


@dataclass
class DremioCredentials(Credentials):
    environment: Optional[str]
    database: Optional[str]
    schema: Optional[str]
    datalake: Optional[str]
    root_path: Optional[str]
    cloud_project_id: Optional[str] = None
    cloud_host: Optional[str] = None
    software_host: Optional[str] = None
    UID: Optional[str] = None
    PWD: Optional[str] = None
    port: Optional[int] = 9047  # for rest endpoint
    use_ssl: Optional[bool] = True
    pat: Optional[str] = None
    additional_parameters: Optional[str] = None

    _ALIASES = {
        "user": "UID",
        "username": "UID",
        "pass": "PWD",
        "password": "PWD",
        "server": "host",
        "track": "environment",
        "space": "database",
        "folder": "schema",
        "materialization_database": "datalake",
        "materialization_schema": "root_path",
    }

    @property
    def type(self):
        return "dremio"

    @property
    def unique_field(self):
        return self.host

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'
        # raise NotImplementedError

        return (
            "driver",
            "cloud_host",
            "cloud_project_id",
            "software_host",
            "port",
            "UID",
            "database",
            "schema",
            "additional_parameters",
            "datalake",
            "root_path",
            "environment",
            "use_ssl",
        )

    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(data)
        if "cloud_host" not in data:
            data["cloud_host"] = None
        if "software_host" not in data:
            data["software_host"] = None
        if "database" not in data:
            data["database"] = None
        if "schema" not in data:
            data["schema"] = None
        if "datalake" not in data:
            data["datalake"] = None
        if "root_path" not in data:
            data["root_path"] = None
        if "environment" not in data:
            data["environment"] = None
        if "pat" not in data:
            data["pat"] = None
        return data

    def __post_init__(self):
        if self.database is None:
            self.database = "@" + self.UID
        if self.schema is None:
            self.schema = DremioRelation.no_schema
        if self.datalake is None:
            self.datalake = "$scratch"
        if self.root_path is None:
            self.root_path = DremioRelation.no_schema


class DremioConnectionManager(SQLConnectionManager):
    TYPE = "dremio"

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

        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials
        api_parameters = DremioConnectionManager.build_api_parameters(credentials)

        try:
            handle = DremioHandle(api_parameters)
            _ = handle.cursor()
            connection.state = "open"
            connection.handle = handle
            logger.debug(f"Connected to db: {credentials.database}")
        except Exception as e:
            logger.debug(f"Could not connect to db: {e}")
            connection.handle = None
            connection.state = "fail"
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

    def add_query(self, sql, auto_begin=True, bindings=None, abridge_sql_log=False):

        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()

        logger.debug(f'Using {self.TYPE} connection "{connection.name}')

        with self.exception_handler(sql):
            if abridge_sql_log:
                logger.debug("On {}: {}....".format(connection.name, sql[0:512]))
            else:
                logger.debug("On {}: {}".format(connection.name, sql))

            pre = time.time()
            cursor = connection.handle.cursor()

            # pyodbc does not handle a None type binding!
            if bindings is None:
                cursor.execute(sql)
            else:
                logger.debug(f"Bindings: {bindings}")
                cursor.execute(sql, bindings)

            logger.debug(
                "SQL status: {} in {:0.2f} seconds".format(
                    self.get_response(cursor), (time.time() - pre)
                )
            )
            return connection, cursor

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_response(cls, cursor: DremioCursor) -> AdapterResponse:
        rows = cursor.rowcount
        message = "OK" if rows == -1 else str(rows)
        return AdapterResponse(_message=message, rows_affected=rows)

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[AdapterResponse, agate.Table]:
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        fetch = True
        if fetch:
            table = cursor.table
        else:
            table = dbt.clients.agate_helper.empty_table()
        cursor.close()

        return response, table

    def drop_catalog(self, database, schema):
        connection = self.get_thread_connection()
        credentials = connection.credentials
        api_parameters = self.build_api_parameters(credentials)

        token = login(api_parameters)
        connection.credentials.token = token

        path = [database]
        folders = schema.split(".")
        path.extend(folders)
        try:
            catalog_info = catalog_item(api_parameters, None, path, False)
        except DremioNotFoundException:
            logger.debug("Catalog not found. Returning")
            return

        delete_catalog(api_parameters, catalog_info["id"], catalog_info["tag"], False)

    def create_catalog(self, database, schema):
        connection = self.get_thread_connection()
        credentials = connection.credentials
        api_parameters = self.build_api_parameters(credentials)

        token = login(api_parameters)
        connection.credentials.token = token

        path = [database]
        folders = schema.split(".")
        path.extend(folders)
        logger.debug(f"The database value is {database}")
        if database == "@" + credentials.UID:
            logger.debug("Database is default: creating folders only")
        else:
            space_json = self._make_new_space_json(database)
            try:
                create_catalog(api_parameters, space_json, False)
            except DremioAlreadyExistsException:
                logger.debug(
                    f"Database {database} already exists. Creating folders only."
                )
        if database != credentials.datalake:
            temp_path = [database]
            for folder in folders:
                temp_path.append(folder)
                folder_json = self._make_new_folder_json(temp_path)
                try:
                    create_catalog(api_parameters, folder_json, False)
                except DremioAlreadyExistsException:
                    logger.debug(f"Folder {folder} already exists.")
        return

    def _make_new_space_json(self, name) -> json:
        python_dict = {"entityType": "space", "name": name}
        return json.dumps(python_dict)

    def _make_new_folder_json(self, path) -> json:
        python_dict = {"entityType": "folder", "path": path}
        return json.dumps(python_dict)

    @classmethod
    def build_api_parameters(cls, credentials):
        def __build_software_base_url(host, port, use_ssl):
            protocol = "http"
            if use_ssl:
                protocol = "https"
            return f"{protocol}://{host}:{port}"

        def __build_cloud_base_url(host):
            protocol = "https"
            return f"{protocol}://{host}"

        api_parameters = None
        dremio_authentication = DremioAuthentication.build(
            credentials.UID, credentials.PWD, credentials.pat
        )

        if credentials.cloud_host != None:
            api_parameters = Parameters(
                __build_cloud_base_url(credentials.cloud_host),
                dremio_authentication,
                is_cloud=True,
                cloud_project_id=credentials.cloud_project_id,
            )
        elif credentials.software_host != None:
            api_parameters = Parameters(
                __build_software_base_url(
                    credentials.software_host, credentials.port, credentials.use_ssl
                ),
                dremio_authentication,
                is_cloud=False,
                cloud_project_id=None,
            )
        else:
            raise dbt.exceptions.DbtProfileError(
                dbt.exceptions.DbtConfigError(
                    "A cloud_host or software_host must be set in project profile."
                )
            )

        return api_parameters
