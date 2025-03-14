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
import os
from dbt.tests.adapter.basic import files
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from tests.fixtures.profiles import unique_schema, dbt_profile_data
from tests.utils.util import (
    BUCKET,
    relation_from_name,
    check_relations_equal
)
from dbt.tests.util import (
    check_result_nodes_by_name,
    get_manifest,
    run_dbt
)


schema_yml = """
version: 2
sources:
  - name: raw

    database: "{{ target.datalake }}"
    schema: "{{ target.root_path }}"
    tables:
      - name: seed
        identifier: "{{ var('seed_name', 'base') }}"
        """


class TestEphemeralDremio(BaseEphemeral):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral.sql": files.base_ephemeral_sql,
            "view_model.sql": files.ephemeral_view_sql,
            "table_model.sql": files.ephemeral_table_sql,
            "schema.yml": schema_yml
        }

    def test_ephemeral(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 1
        check_result_nodes_by_name(results, ["base"])

        # run command
        results = run_dbt(["run"])
        assert len(results) == 2
        check_result_nodes_by_name(results, ["view_model", "table_model"])

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation}", fetch="one"
        )
        assert result[0] == 10

        # relations equal
        check_relations_equal(project.adapter, ["base", "view_model", "table_model"])

        # catalog node count
        catalog = run_dbt(["docs", "generate"])
        catalog_path = os.path.join(project.project_root, "target", "catalog.json")
        assert os.path.exists(catalog_path)
        assert len(catalog.nodes) == 3

        # manifest (not in original)
        manifest = get_manifest(project.project_root)
        assert len(manifest.nodes) == 4
        assert len(manifest.sources) == 1
