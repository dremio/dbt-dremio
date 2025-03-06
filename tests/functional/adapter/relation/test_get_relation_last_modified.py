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
from tests.utils.util import BUCKET, relation_from_name


freshness_via_metadata_schema_yml = """version: 2
sources:
  - name: test_source
    freshness:
      warn_after: {count: 10, period: hour}
      error_after: {count: 1, period: day}
    database: "{{ target.datalake }}"  
    schema: "{{ target.schema }}"
    tables:
      - name: test_source
"""


seed = """
id,name
1,Martin
2,Jeter
3,Ruth
4,Gehrig
5,DiMaggio
6,Torre
7,Mantle
8,Berra
9,Maris
""".strip()


class TestGetLastRelationModified:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "+twin_strategy": "prevent",
            },
        }

    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        # We only want the last part of the name
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

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": freshness_via_metadata_schema_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"test_source.csv": seed}

    def test_get_last_relation_modified(self, project):

        # run command
        result = run_dbt(["seed"])
        relation = relation_from_name(project.adapter, "test_source")
        result = project.run_sql(
            f"INSERT INTO {relation} VALUES (10, 'name')", fetch="one"
        )
        results = run_dbt(["source", "freshness"])
        assert len(results) == 1
        result = results[0]
        assert result.status == "pass"
