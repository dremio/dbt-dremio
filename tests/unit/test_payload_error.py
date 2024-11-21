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

from unittest import mock, TestCase
from dbt.adapters.dremio.api.cursor import DremioCursor
from dbt.adapters.dremio.api.parameters import Parameters
from dbt.adapters.dremio.api.authentication import DremioAuthentication


class TestPayloadErrorMessage(TestCase):
    @mock.patch("dbt.adapters.dremio.api.cursor.job_status")
    def test_payload_error(self, mocked_job_status):
        dremio_cursor_object = DremioCursor(
            Parameters("base_url", DremioAuthentication)
        )
        mocked_job_status.return_value = {
            "jobState": "FAILED",
            "errorMessage": "ERROR: Expected error message",
        }
        with self.assertRaises(Exception, msg="ERROR: Expected error message"):
            dremio_cursor_object._populate_rowcount()
