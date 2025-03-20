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
from dbt.tests.adapter.utils.data_types.test_type_boolean import BaseTypeBoolean
from tests.fixtures.profiles import unique_schema, dbt_profile_data
from tests.utils.util import (
    check_relations_equal,
    get_relation_columns
)
from dbt.tests.util import (
    run_dbt
)


models__actual_sql = """
select cast('True' as {{ type_boolean() }}) as boolean_col
"""

class TestTypeBoolean(BaseTypeBoolean):
    @pytest.fixture(scope="class")
    def models(self):
        return {"actual_view.sql": self.interpolate_macro_namespace(models__actual_sql, "type_boolean")}

    def test_check_types_assert_match(self, project):
        run_dbt(["build"])

        # check contents equal
        check_relations_equal(project.adapter, ["expected", "actual_view"])

        # check types equal
        expected_cols = get_relation_columns(project.adapter, "expected")
        actual_cols = get_relation_columns(project.adapter, "actual_view")
        print(f"Expected columns: {expected_cols}")
        print(f"Actual columns: {actual_cols}")
        self.assert_columns_equal(project, expected_cols, actual_cols)
