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

import pytest
from math import ceil
from unittest.mock import patch
from dbt.adapters.dremio.api.cursor import DremioCursor
from dbt.adapters.dremio.api.parameters import Parameters
from dbt.adapters.dremio.api.authentication import DremioAuthentication


class TestJobResults:
    mocked_job_results_dict = [
        {
            "rowCount": 7,
            "schema": [{"name": "id", "type": {"name": "BIGINT"}}],
            "rows": [{"id": 1}, {"id": 2}],
        },
        {
            "rowCount": 7,
            "schema": [{"name": "id", "type": {"name": "BIGINT"}}],
            "rows": [{"id": 3}, {"id": 4}],
        },
        {
            "rowCount": 7,
            "schema": [{"name": "id", "type": {"name": "BIGINT"}}],
            "rows": [{"id": 5}, {"id": 6}],
        },
        {
            "rowCount": 7,
            "schema": [{"name": "id", "type": {"name": "BIGINT"}}],
            "rows": [{"id": 7}],
        },
        {  # Something is wrong if it reaches here
            "rowCount": 0,
            "schema": [{"name": "id", "type": {"name": "BIGINT"}}],
            "rows": [{"id": 42}],
        },
    ]

    expected_combined_job_results = {
        "rowCount": 7,
        "schema": [{"name": "id", "type": {"name": "BIGINT"}}],
        "rows": [
            {"id": 1},
            {"id": 2},
            {"id": 3},
            {"id": 4},
            {"id": 5},
            {"id": 6},
            {"id": 7},
        ],
    }

    @patch("dbt.adapters.dremio.api.cursor.job_results")
    def test_job_result_pagination(self, mocked_job_results_func):
        # Arrange
        ROW_LIMIT = 2
        JOB_RESULT_TOTAL_CALLS = ceil(
            self.mocked_job_results_dict[0]["rowCount"] / ROW_LIMIT
        )
        dremio_cursor_obj = DremioCursor(Parameters("hello", DremioAuthentication()))
        mocked_job_results_func.side_effect = self.mocked_job_results_dict

        # Act
        dremio_cursor_obj._populate_job_results(row_limit=ROW_LIMIT)

        # Assert
        assert dremio_cursor_obj._job_results == self.expected_combined_job_results
        assert mocked_job_results_func.call_count == JOB_RESULT_TOTAL_CALLS
