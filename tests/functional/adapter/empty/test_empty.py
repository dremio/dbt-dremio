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
from dbt.tests.adapter.empty.test_empty import BaseTestEmpty
from dbt.tests.adapter.empty import _models
from tests.fixtures.profiles import unique_schema, dbt_profile_data
from dbt.tests.util import run_dbt

schema_sources_yml = """
sources:
  - name: seed_sources
    database: "dbt_test_source"
    schema: "{{ target.root_path }}"
    tables:
      - name: raw_source
"""


class TestDremioEmpty(BaseTestEmpty):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"+twin_strategy": "allow"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_input.sql": _models.model_input_sql,
            "ephemeral_model_input.sql": _models.ephemeral_model_input_sql,
            "model.sql": _models.model_sql,
            "sources.yml": schema_sources_yml,
        }

    def test_run_with_empty(self, project):
        # create source from seed
        run_dbt(["seed"])

        # run without empty - 3 expected rows in output - 1 from each input
        # run_dbt(["run"])
        # self.assert_row_count(project, "model", 3)

        # run with empty - 0 expected rows in output
        run_dbt(["build", "--empty"])
        self.assert_row_count(project, "model", 0)
