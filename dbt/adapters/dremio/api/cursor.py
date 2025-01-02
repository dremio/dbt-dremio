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


import time

import agate

from dbt.adapters.dremio.api.rest.client import DremioRestClient

from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("dremio")


class DremioCursor:
    def __init__(self, rest_client: DremioRestClient):
        self._rest_client = rest_client

        self._closed = False
        self._job_id = None
        self._rowcount = -1
        self._job_results = None
        self._table_results: agate.Table = None
        self._description = None

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, value):
        self._description = value

    @property
    def closed(self):
        return self._closed

    @closed.setter
    def closed(self, new_closed_value):
        self._closed = new_closed_value

    @property
    def rowcount(self):
        return self._rowcount

    @property
    def table(self) -> agate.Table:
        return self._table_results

    def job_results(self):
        if self.closed:
            raise Exception("CursorClosed")
        if self._job_results is None:
            self._populate_job_results()

        return self._job_results

    def job_cancel(self):
        # cancels current job
        logger.debug(f"Cancelling job {self._job_id}")
        return self._rest_client.job_cancel_api(self._job_id)

    def close(self):
        if self.closed:
            raise Exception("CursorClosed")
        self._initialize()
        self.closed = True

    def execute(self, sql, bindings=None, fetch=False):
        if self.closed:
            raise Exception("CursorClosed")
        if bindings is None:
            self._initialize()

            json_payload = self._rest_client.sql_endpoint(sql, context=None)

            self._job_id = json_payload["id"]

            self._populate_rowcount()
            if fetch:
                self._populate_job_results()
            self._populate_results_table()

        else:
            raise Exception("Bindings not currently supported.")

    def fetchone(self):
        row = None
        if self._table_results is not None:
            row = self._table_results.rows[0]
        return row

    def fetchall(self):
        logger.debug(f"The fetch result is: {self._table_results.rows}")
        return self._table_results.rows

    def _initialize(self):
        self._job_id = None
        self._rowcount = -1
        self._table_results = None
        self._job_results = None

    def _populate_rowcount(self):
        if self.closed:
            raise Exception("CursorClosed")
        # keep checking job status until status is one of COMPLETE, CANCELLED or FAILED
        # map job results to AdapterResponse
        job_id = self._job_id

        last_job_state = ""
        job_status_response = self._rest_client.job_status(job_id)
        job_status_state = job_status_response["jobState"]

        while True:
            time.sleep(0.2)
            if job_status_state != last_job_state:
                logger.debug(f"Job State = {job_status_state}")

            if job_status_state == "FAILED":
                error_message = job_status_response["errorMessage"]
                raise Exception(f"ERROR: {error_message}")

            if job_status_state == "COMPLETED" or job_status_state == "CANCELLED":
                break
            last_job_state = job_status_state
            job_status_response = self._rest_client.job_status(job_id)
            job_status_state = job_status_response["jobState"]

        # this is done as job status does not return a rowCount if there are no rows affected (even in completed job_state)
        # pyodbc Cursor documentation states "[rowCount] is -1 if no SQL has been executed or if the number of rows is unknown.
        # Note that it is not uncommon for databases to report -1 immediately after a SQL select statement for performance reasons."
        if "rowCount" not in job_status_response:
            rows = -1
            logger.debug("rowCount does not exist in job_status payload")
        else:
            rows = job_status_response["rowCount"]

        self._rowcount = rows

    def _populate_job_results(self, row_limit=500):
        if self._job_results == None:
            combined_job_results = self._rest_client.job_results(
                self._job_id,
                offset=0,
                limit=row_limit,
            )
            total_row_count = combined_job_results["rowCount"]
            current_row_count = len(combined_job_results["rows"])

            if total_row_count > 100000:
                logger.warning(
                    "Fetching more than 100000 records. This may result in slower performance."
                )

            while current_row_count < total_row_count:
                combined_job_results["rows"].extend(
                    self._rest_client.job_results(
                        self._job_id,
                        offset=current_row_count,
                        limit=row_limit,
                    )["rows"]
                )
                current_row_count += row_limit

            self._job_results = combined_job_results

    def _populate_results_table(self):
        if self._job_results is not None:
            tester = agate.TypeTester()
            json_rows = self._job_results["rows"]
            self._table_results = json_rows
            for col in self._job_results["schema"]:
                name = col["name"]
                data_type_str = col["type"]["name"]
                if data_type_str == "BIGINT":
                    tester = agate.TypeTester(force={f"{name}": agate.Number()})

            self._table_results = agate.Table.from_object(
                json_rows, column_types=tester
            )
