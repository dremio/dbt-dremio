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
from dbt_common.exceptions import CompilationError
from tests.fixtures.profiles import unique_schema, dbt_profile_data

model_sql = """
select 1 as id
"""

fail_macros__failure_sql = """
{% macro get_catalog_relations(information_schema, relations) %}
    {% do exceptions.raise_compiler_error('rejected: no catalogs for you') %}
{% endmacro %}

"""


class TestDocsGenerateOverride:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": model_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"failure.sql": fail_macros__failure_sql}

    def test_override_used(
        self,
        project,
    ):
        results = run_dbt(["run"])
        assert len(results) == 1
        # this should pick up our failure macro and raise a compilation exception
        with pytest.raises(CompilationError) as excinfo:
            run_dbt(["--warn-error", "docs", "generate"])
        assert "rejected: no catalogs for you" in str(excinfo.value)
