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
from dbt.tests.adapter.utils.test_timestamps import BaseCurrentTimestamps
from tests.fixtures.profiles import unique_schema, dbt_profile_data


class TestCurrentTimestampDremio(BaseCurrentTimestamps):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "get_current_timestamp.sql": 'select {{ current_timestamp() }} as "current_timestamp"'
        }

    @pytest.fixture(scope="class")
    def expected_schema(self):
        return {"current_timestamp": "timestamp"}

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return '''select (SELECT CURRENT_TIMESTAMP()) as "current_timestamp"'''
