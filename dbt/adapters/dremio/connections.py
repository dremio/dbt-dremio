# Copyright (C) 2022 Dremio Corporation

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import agate
from typing import Tuple
from contextlib import contextmanager

from dbt.adapters.dremio.api.cursor import DremioCursor
from dbt.adapters.dremio.api.handle import DremioHandle
from dbt.adapters.dremio.api.parameters import ParametersBuilder

import time
import json

import dbt.exceptions
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse

from dbt.adapters.dremio.api.rest.endpoints import (
    delete_catalog,
    create_catalog_api,
    get_catalog_item,
)
from dbt.adapters.dremio.api.rest.error import (
    DremioAlreadyExistsException,
    DremioNotFoundException,
    DremioRequestTimeoutException,
    DremioTooManyRequestsException,
    DremioInternalServerException,
    DremioServiceUnavailableException,
    DremioGatewayTimeoutException,
)

from dbt.events import AdapterLogger

logger = AdapterLogger("dremio")


class DremioConnectionManager(SQLConnectionManager):
    TYPE = "dremio"
    DEFAULT_CONNECTION_RETRIES = 1

    retries = DEFAULT_CONNECTION_RETRIES

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except Exception as e:
            logger.debug(f"Error running SQL: {sql}")
            self.release()
            if isinstance(e, dbt.exceptions.DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt.exceptions.DbtRuntimeError(e)

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials
        parameters_builder = ParametersBuilder.build(credentials)
        api_parameters = parameters_builder.get_parameters()

        def connect():
            handle = DremioHandle(api_parameters)
            _ = handle.cursor()
            connection.state = "open"
            connection.handle = handle
            logger.debug(f"Connected to db: {credentials.database}")
            return handle

        retryable_exceptions = [
            # list of retryable_exceptions underlying driver might expose
            DremioRequestTimeoutException,
            DremioTooManyRequestsException,
            DremioInternalServerException,
            DremioServiceUnavailableException,
            DremioGatewayTimeoutException,
        ]

        def exponential_backoff_retry_timeout(retries: int) -> int:
            BASE = 2  # multiplicative factor
            time_delay = pow(BASE, retries)
            return time_delay

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=cls.retries,
            retry_timeout=exponential_backoff_retry_timeout,
            retryable_exceptions=retryable_exceptions,
        )

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
        pass

    def add_commit_query(self):
        pass

    # Auto_begin may not be relevant with the rest_api
    def add_query(
        self, sql, auto_begin=True, bindings=None, abridge_sql_log=False, fetch=False
    ):
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()

        logger.debug(f'Using {self.TYPE} connection "{connection.name}". fetch={fetch}')

        with self.exception_handler(sql):
            if abridge_sql_log:
                logger.debug("On {}: {}....".format(connection.name, sql[0:512]))
            else:
                logger.debug("On {}: {}".format(connection.name, sql))

            pre = time.time()
            cursor = connection.handle.cursor()

            if bindings is None:
                cursor.execute(sql, fetch=fetch)
            else:
                logger.debug(f"Bindings: {bindings}")
                cursor.execute(sql, bindings, fetch=fetch)

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
        _, cursor = self.add_query(sql, auto_begin, fetch=fetch)
        response = self.get_response(cursor)
        if fetch:
            table = cursor.table
        else:
            table = dbt.clients.agate_helper.empty_table()

        return response, table

    def drop_catalog(self, database, schema):
        logger.debug('Dropping schema "{}.{}"', database, schema)

        thread_connection = self.get_thread_connection()
        connection = self.open(thread_connection)
        credentials = connection.credentials
        api_parameters = connection.handle.get_parameters()

        path_list = self._create_path_list(database, schema)
        if database != credentials.datalake:
            try:
                catalog_info = get_catalog_item(
                    api_parameters,
                    catalog_id=None,
                    catalog_path=path_list,
                )
            except DremioNotFoundException:
                logger.debug("Catalog not found. Returning")
                return

            delete_catalog(api_parameters, catalog_info["id"])

    def create_catalog(self, database, schema):
        thread_connection = self.get_thread_connection()
        connection = self.open(thread_connection)
        credentials = connection.credentials
        api_parameters = connection.handle.get_parameters()

        if database == ("@" + credentials.UID):
            logger.debug("Database is default: creating folders only")
        else:
            self._create_space(database, api_parameters)
        if database != credentials.datalake:
            self._create_folders(database, schema, api_parameters)
        return

    def _make_new_space_json(self, name) -> json:
        python_dict = {"entityType": "space", "name": name}
        return json.dumps(python_dict)

    def _make_new_folder_json(self, path) -> json:
        python_dict = {"entityType": "folder", "path": path}
        return json.dumps(python_dict)

    def _create_space(self, database, api_parameters):
        space_json = self._make_new_space_json(database)
        try:
            create_catalog_api(api_parameters, space_json)
        except DremioAlreadyExistsException:
            logger.debug(f"Database {database} already exists. Creating folders only.")

    def _create_folders(self, database, schema, api_parameters):
        temp_path_list = [database]
        for folder in schema.split("."):
            temp_path_list.append(folder)
            folder_json = self._make_new_folder_json(temp_path_list)
            try:
                create_catalog_api(api_parameters, folder_json)
            except DremioAlreadyExistsException:
                logger.debug(f"Folder {folder} already exists.")

    def _create_path_list(self, database, schema):
        path = [database]
        folders = schema.split(".")
        path.extend(folders)
        return path
