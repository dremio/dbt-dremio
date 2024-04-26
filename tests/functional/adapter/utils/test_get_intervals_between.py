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
from dbt.tests.adapter.utils.test_get_intervals_between import BaseGetIntervalsBetween
from dbt.tests.adapter.utils.fixture_get_intervals_between import (
    models__test_get_intervals_between_yml,
)
from tests.utils.util import BUCKET
from tests.fixtures.profiles import unique_schema, dbt_profile_data

models__test_get_intervals_between_sql = """
SELECT
    {{get_intervals_between("'2023-09-01'", "'2023-09-12'", 'day') }} as intervals,
    11 as expected

"""


class TestGetIntervalsBetweenDremio(BaseGetIntervalsBetween):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_get_intervals_between.yml": models__test_get_intervals_between_yml,
            "test_get_intervals_between.sql": self.interpolate_macro_namespace(
                models__test_get_intervals_between_sql,
                "get_intervals_between",
            ),
        }
