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
from dbt.tests.util import run_dbt
from tests.utils.util import SOURCE


# UDF with pre-hook that creates a view and post-hook that drops it
udf_with_hooks_sql = """
{{ config(
    materialized='udf',
    parameter_list='x INT',
    returns='INT',
    pre_hook="CREATE VIEW {{ target.database }}.{{ this.schema }}.hook_test_view AS SELECT 1 as id",
    post_hook="DROP VIEW {{ target.database }}.{{ this.schema }}.hook_test_view"
) }}

RETURN x * 2
"""

# Multiple hooks test
udf_with_multiple_hooks_sql = """
{{ config(
    materialized='udf',
    parameter_list='x INT, y INT',
    returns='INT',
    pre_hook=[
        "CREATE VIEW {{ target.database }}.{{ this.schema }}.hook_test_view_1 AS SELECT 1 as id",
        "CREATE VIEW {{ target.database }}.{{ this.schema }}.hook_test_view_2 AS SELECT 2 as id"
    ],
    post_hook=[
        "DROP VIEW {{ target.database }}.{{ this.schema }}.hook_test_view_1",
        "DROP VIEW {{ target.database }}.{{ this.schema }}.hook_test_view_2"
    ]
) }}

RETURN x + y
"""


class TestUDFHooks:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "udf_with_hooks.sql": udf_with_hooks_sql,
            "udf_with_multiple_hooks.sql": udf_with_multiple_hooks_sql,
        }

    def test_udf_single_hook_execution(self, project):
        # Run the UDF with hooks
        results = run_dbt(["run", "--select", "udf_with_hooks"])
        assert len(results) == 1

        # If this succeeded, it means:
        # 1. Pre-hook created the table
        # 2. UDF was created successfully
        # 3. Post-hook dropped the table

        # Run it again to ensure table was properly cleaned up
        # (would fail if table still exists)
        results = run_dbt(["run", "--select", "udf_with_hooks"])
        assert len(results) == 1

    def test_udf_multiple_hooks_execution(self, project):
        # Run the UDF with multiple hooks
        results = run_dbt(["run", "--select", "udf_with_multiple_hooks"])
        assert len(results) == 1

        # Run it again to ensure both tables were cleaned up
        results = run_dbt(["run", "--select", "udf_with_multiple_hooks"])
        assert len(results) == 1
