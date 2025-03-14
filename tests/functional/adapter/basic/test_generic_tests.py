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
from dbt.tests.adapter.basic import files
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from tests.fixtures.profiles import unique_schema, dbt_profile_data


schema_base_yml = """
version: 2
sources:
  - name: raw
    database: "{{ target.datalake}}"
    schema: "{{ target.root_path }}"
    tables:
      - name: seed
        identifier: "{{ var('seed_name', 'base') }}"
"""

class TestGenericTestsDremio(BaseGenericTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": files.base_view_sql,
            "table_model.sql": files.base_table_sql,
            "schema.yml": schema_base_yml,
            "schema_view.yml": files.generic_test_view_yml,
            "schema_table.yml": files.generic_test_table_yml,
        }
