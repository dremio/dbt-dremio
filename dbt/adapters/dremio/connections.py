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
from typing import Tuple, Optional, List
from contextlib import contextmanager

from dbt.adapters.dremio.api.cursor import DremioCursor
from dbt.adapters.dremio.api.handle import DremioHandle
from dbt.adapters.dremio.api.parameters import ParametersBuilder
from dbt.adapters.dremio.api.rest.entities.reflection import ReflectionEntity
from dbt.adapters.dremio.relation import DremioRelation

from dbt_common.clients import agate_helper

import time
import json

import dbt_common.exceptions
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.contracts.connection import AdapterResponse

from dbt.adapters.dremio.api.rest.client import DremioRestClient

from dbt.adapters.dremio.api.rest.error import (
    DremioAlreadyExistsException,
    DremioNotFoundException,
    DremioRequestTimeoutException,
    DremioTooManyRequestsException,
    DremioInternalServerException,
    DremioServiceUnavailableException,
    DremioGatewayTimeoutException,
    DremioBadRequestException,
)

from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("dremio")

class DremioConnectionManager(SQLConnectionManager):
    TYPE = "dremio"
    DEFAULT_CONNECTION_RETRIES = 5

    retries = DEFAULT_CONNECTION_RETRIES

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except Exception as e:
            logger.debug(f"Error running SQL: {sql}")
            self.release()
            if isinstance(e, dbt_common.exceptions.DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt_common.exceptions.DbtRuntimeError(e)

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
        self, sql, auto_begin=True, bindings=None, abridge_sql_log=False,
        fetch=False
    ):
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()

        logger.debug(
            f'Using {self.TYPE} connection "{connection.name}". fetch={fetch}')

        with self.exception_handler(sql):
            if abridge_sql_log:
                logger.debug(
                    "On {}: {}....".format(connection.name, sql[0:512]))
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
            self,
            sql: str,
            auto_begin: bool = False,
            fetch: bool = False,
            limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, agate.Table]:
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin, fetch=fetch)
        response = self.get_response(cursor)
        if fetch:
            table = cursor.table
        else:
            table = agate_helper.empty_table()

        return response, table

    def drop_catalog(self, database, schema):
        logger.debug('Dropping schema "{}.{}"', database, schema)

        thread_connection = self.get_thread_connection()
        connection = self.open(thread_connection)
        credentials = connection.credentials
        rest_client = connection.handle.get_client()

        path_list = self._create_path_list(database, schema)
        if database != credentials.datalake:
            try:
                catalog_info = rest_client.get_catalog_item(
                    catalog_id=None,
                    catalog_path=path_list,
                )
            except DremioNotFoundException:
                logger.debug("Catalog not found. Returning")
                return

            rest_client.delete_catalog(catalog_info["id"])

    def create_catalog(self, relation):
        thread_connection = self.get_thread_connection()
        connection = self.open(thread_connection)
        credentials = connection.credentials
        rest_client = connection.handle.get_client()
        database = relation.database
        schema = relation.schema

        if database == ("@" + credentials.UID):
            logger.debug("Database is default: creating folders only")
        else:
            logger.debug(f"Creating space: {database}")
            self._create_space(database, rest_client)

        if database != credentials.datalake:
            logger.debug(f"Creating folder(s): {database}.{schema}")
            self._create_folders(database, schema, rest_client)
        return
    
    # dbt docs integration with Dremio wikis and tags
    def process_wikis(self, relation, text: str):
        logger.debug("Integrating wikis")
        thread_connection = self.get_thread_connection()
        connection = self.open(thread_connection)
        rest_client = connection.handle.get_client()
        database = relation.database
        schema = relation.schema

        path = self._create_path_list(database,schema)
        identifier = relation.identifier
        path.append(identifier)
        try:
            catalog_info = rest_client.get_catalog_item(
                catalog_id=None,
                catalog_path=path,
            )
        except DremioNotFoundException:
            logger.debug("Catalog not found. Returning")
            return

        object_id = catalog_info.get("id")
        stored_wiki = rest_client.retrieve_wiki(object_id)
        wiki_content = stored_wiki.get("text")
        wiki_version = stored_wiki.get("version", None)

        if wiki_version is None:
            logger.debug(f"Creating wiki for {'.'.join(path)}")
            result = rest_client.create_wiki(object_id, text)
            logger.debug(result)
            return
        
        if wiki_content != text:
            if text == "": # text is empty, delete wiki
                logger.debug(f"Deleting wiki for {'.'.join(path)}")
                result = rest_client.delete_wiki(object_id, wiki_version)
                logger.debug(result)
                return
            
            logger.debug(f"Updating wiki for {'.'.join(path)}")
            result = rest_client.update_wiki(object_id, text, wiki_version)
            logger.debug(result)

    def process_tags(self, relation, tags: list[str]):
        logger.debug("Integrating tags")
        thread_connection = self.get_thread_connection()
        connection = self.open(thread_connection)
        rest_client = connection.handle.get_client()
        database = relation.database
        schema = relation.schema

        path = self._create_path_list(database,schema)
        identifier = relation.identifier
        path.append(identifier)
        try:
            catalog_info = rest_client.get_catalog_item(
                catalog_id=None,
                catalog_path=path,
            )
        except DremioNotFoundException:
            logger.debug("Catalog not found. Returning")
            return

        object_id = catalog_info.get("id")
        stored_tags = rest_client.retrieve_tags(object_id)
        tags_list = stored_tags.get("tags")
        tags_version = stored_tags.get("version", None)

        if tags_version is None:
            logger.debug(f"Creating tags for {'.'.join(path)}")
            result = rest_client.create_tags(object_id, tags)
            logger.debug(result)
            return

        if tags_list != tags:
            if tags == []:  # tags is empty, delete tags
                logger.debug(f"Deleting tags for {'.'.join(path)}")
                result = rest_client.delete_tags(object_id, tags_version)
                logger.debug(result)
                return

            logger.debug(f"Updating tags for {'.'.join(path)}")
            result = rest_client.update_tags(object_id, tags, tags_version)
            logger.debug(result)


    def create_reflection(self, name: str, reflection_type: str, anchor: DremioRelation, display: List[str],
                          dimensions: List[str],
                          date_dimensions: List[str], measures: List[str],
                          computations: List[str], partition_by: List[str], partition_transform: List[str],
                          partition_method: str, distribute_by: List[str], localsort_by: List[str],
                          arrow_cache: bool) -> None:
        thread_connection = self.get_thread_connection()
        connection = self.open(thread_connection)
        rest_client = connection.handle.get_client()

        database = anchor.database
        schema = anchor.schema
        path = self._create_path_list(database, schema)
        identifier = anchor.identifier

        path.append(identifier)

        catalog_info = rest_client.get_catalog_item(
            catalog_id=None,
            catalog_path=path,
        )

        dataset_id = catalog_info.get("id")

        payload = ReflectionEntity(name, reflection_type, dataset_id, display, dimensions, date_dimensions, measures,
                                   computations, partition_by, partition_transform, partition_method, distribute_by,
                                   localsort_by, arrow_cache).build_payload()

        dataset_info = rest_client.get_reflections(dataset_id)
        reflections_info = dataset_info.get("data")

        updated = False
        for reflection in reflections_info:
            if reflection.get("name") == name:
                logger.debug(f"Reflection {name} already exists. Updating it")
                payload["tag"] = reflection.get("tag")
                rest_client.update_reflection(reflection.get("id"), payload)
                updated = True
                break

        if not updated:
            logger.debug(f"Reflection {name} does not exist. Creating it")
            rest_client.create_reflection(payload)

    def _make_new_space_json(self, name) -> json:
        python_dict = {"entityType": "space", "name": name}
        return json.dumps(python_dict)

    def _make_new_folder_json(self, path) -> json:
        python_dict = {"entityType": "folder", "path": path}
        return json.dumps(python_dict)

    def _create_space(self, database, rest_client: DremioRestClient):
        space_json = self._make_new_space_json(database)
        try:
            rest_client.create_catalog_api(space_json)
        except DremioAlreadyExistsException:
            logger.debug(
                f"Database {database} already exists. Creating folders only.")

    def _create_folders(self, database, schema, rest_client: DremioRestClient):
        temp_path_list = [database]
        for folder in schema.split("."):
            temp_path_list.append(folder)
            folder_json = self._make_new_folder_json(temp_path_list)
            try:
                rest_client.create_catalog_api(folder_json)
            except DremioAlreadyExistsException:
                logger.debug(f"Folder {folder} already exists.")
            except DremioBadRequestException as e:
                if "Can not create a folder inside a [SOURCE]" in e.message:
                    logger.debug(f"Ignoring {e}")
                else:
                    raise e

    def _create_path_list(self, database, schema):
        path = [database]
        if schema != 'no_schema':
            folders = schema.split(".")
            path.extend(folders)
        return path
