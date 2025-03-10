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
from dbt.tests.adapter.utils.test_array_concat import BaseArrayConcat
from tests.utils.util import BUCKET

models__array_concat_expected_sql = """
select 1 as id, array[1,2,3,4,5,6] as array_col union all
select 2 as id, array[2,3] as array_col union all
select 3 as id, array[3,2] as array_col
"""

models__array_concat_actual_sql = """
select 1 as id, array_concat(array[1,2,3], array[4,5,6]) as array_col union all
select 2 as id, array_concat(array[2], array[3]) as array_col union all
select 3 as id, array_concat(array[3], array[2]) as array_col
"""


class TestArrayConcat(BaseArrayConcat):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": models__array_concat_actual_sql,
            "expected.sql": models__array_concat_expected_sql,
        }

    # Override this fixture to prepend our schema with BUCKET
    # This ensures the schema works with our datalake
    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        test_file = test_file.split(".")[-1]
        unique_schema = f"{BUCKET}.{prefix}_{test_file}"
        return unique_schema

    @pytest.fixture(scope="class")
    def dbt_profile_data(
        self, unique_schema, dbt_profile_target, profiles_config_update
    ):
        profile = {
            "test": {
                "outputs": {
                    "default": {},
                },
                "target": "default",
            },
        }
        target = dbt_profile_target
        target["schema"] = unique_schema
        target["root_path"] = unique_schema
        profile["test"]["outputs"]["default"] = target

        if profiles_config_update:
            profile.update(profiles_config_update)
        return profile
