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
from unittest.mock import MagicMock, patch
from dbt_common.exceptions import DbtRuntimeError
from dbt.adapters.dremio.api.rest.error import (
    DremioAlreadyExistsException,
    DremioRequestTimeoutException,
)
from dbt.adapters.dremio.connections import DremioConnectionManager


class TestRetryConnection:
    @patch("dbt.adapters.dremio.api.rest.client._post")
    @patch("dbt.adapters.contracts.connection.Connection")
    # When you nest patch decorators the mocks are passed in to the decorated function in bottom up order.
    def test_connection_retry(
        self,
        mocked_connection_obj,
        mocked_post_func,
    ):
        # Arrange
        TOTAL_CONNECTION_ATTEMPTS = (
            DremioConnectionManager.DEFAULT_CONNECTION_RETRIES + 1
        )

        mocked_connection_obj.credentials.software_host = ""
        mocked_connection_obj.credentials.cloud_host = None
        mocked_connection_obj.credentials.port = 9047
        mocked_connection_obj.credentials.use_ssl = False

        mocked_post_func.side_effect = DremioRequestTimeoutException(
            msg="Request timeout: Test",
            original_exception="408 original exception",
        )

        # Act
        with pytest.raises(DbtRuntimeError):
            DremioConnectionManager.open(connection=mocked_connection_obj)

        # Assert
        assert mocked_post_func.call_count == TOTAL_CONNECTION_ATTEMPTS


class TestCreateFolders:
    def test_create_folders_swallows_already_exists(self):
        # Guards the integration path: _check_error normalizes Cloud's
        # 400 "already exists" to DremioAlreadyExistsException, which the
        # existing handler in _create_folders is responsible for swallowing.
        mgr = DremioConnectionManager.__new__(DremioConnectionManager)
        mgr._make_new_folder_json = MagicMock(return_value="{}")
        rest_client = MagicMock()
        rest_client.create_catalog_api.side_effect = DremioAlreadyExistsException(
            msg="Already exists:",
            original_exception="400 Client Error: Bad Request",
        )

        mgr._create_folders("mySource", "staging.subfolder", rest_client)

        # Both folders in the path attempted; both swallowed
        assert rest_client.create_catalog_api.call_count == 2
