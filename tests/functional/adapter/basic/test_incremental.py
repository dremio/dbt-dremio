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
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
)
from tests.fixtures.profiles import unique_schema, dbt_profile_data
from dbt.tests.adapter.incremental.test_incremental_merge_exclude_columns import (
    BaseMergeExcludeColumns,
)
from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
)
from tests.utils.util import (
    BUCKET,
    SOURCE,
    relation_from_name,
    check_relations_equal
)
from dbt.tests.util import run_dbt
from dbt.tests.adapter.basic import files
from collections import namedtuple


models__merge_exclude_columns_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy='merge',
    merge_exclude_columns='msg'
) }}

{% if not is_incremental() %}

-- data for first invocation of model

select 1 as id, 'hello' as msg, 'blue' as color
union all
select 2 as id, 'goodbye' as msg, 'red' as color

{% else %}

-- data for subsequent incremental update

select 1 as id, 'hey' as msg, 'blue' as color
union all
select 2 as id, 'yo' as msg, 'green' as color
union all
select 3 as id, 'anyway' as msg, 'purple' as color

{% endif %}
"""

ResultHolder = namedtuple(
    "ResultHolder",
    [
        "seed_count",
        "model_count",
        "seed_rows",
        "inc_test_model_count",
        "relation",
    ],
)


schema_base_yml = """
version: 2
sources:
  - name: raw

    database: "{{ target.datalake }}"
    schema: "{{ target.root_path }}"
    tables:
      - name: seed
        identifier: "{{ var('seed_name', 'base') }}"
        """


# Need to modify test to not assert any sources for it to pass
class TestIncrementalDremio(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental.sql": files.incremental_sql, "schema.yml": schema_base_yml}

    def test_incremental(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation}", fetch="one"
        )
        assert result[0] == 10

        # added table rowcount
        relation = relation_from_name(project.adapter, "added")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation}", fetch="one"
        )
        assert result[0] == 20

        # run command
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: base"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["base", "incremental"])

        # change seed_name var
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: added"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["added", "incremental"])

        # get catalog from docs generate
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 3


class TestBaseIncrementalNotSchemaChange(BaseIncrementalNotSchemaChange):
    pass

class TestIncrementalOnSchemaChange(BaseIncrementalOnSchemaChange):
    def run_twice_and_assert(self, include, compare_source, compare_target, project):
        # dbt run (twice)
        run_args = ["run"]
        if include:
            run_args.extend(("--select", include))
        results_one = run_dbt(run_args)
        assert len(results_one) == 3

        results_two = run_dbt(run_args)
        assert len(results_two) == 3

        check_relations_equal(project.adapter, [compare_source, compare_target])

    def run_incremental_sync_all_columns(self, project):
        select = "model_a incremental_sync_all_columns incremental_sync_all_columns_target"
        compare_source = "incremental_sync_all_columns"
        compare_target = "incremental_sync_all_columns_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def run_incremental_sync_remove_only(self, project):
        select = "model_a incremental_sync_remove_only incremental_sync_remove_only_target"
        compare_source = "incremental_sync_remove_only"
        compare_target = "incremental_sync_remove_only_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)


class TestBaseMergeExcludeColumnsDremio(BaseMergeExcludeColumns):
    def get_test_fields(self, project, seed, incremental_model, update_sql_file):
        seed_count = len(run_dbt(["seed", "--select", seed, "--full-refresh"]))

        model_count = len(
            run_dbt(["run", "--select", incremental_model, "--full-refresh"])
        )

        relation = incremental_model
        # update seed in anticipation of incremental model update
        row_count_query = "select * from {}.{}".format(
            f"{SOURCE}.{BUCKET}.{project.test_schema}", seed
        )

        seed_rows = len(project.run_sql(row_count_query, fetch="all"))

        # propagate seed state to incremental model according to unique keys
        inc_test_model_count = self.update_incremental_model(
            incremental_model=incremental_model
        )

        return ResultHolder(
            seed_count, model_count, seed_rows, inc_test_model_count, relation
        )

    def check_scenario_correctness(self, expected_fields, test_case_fields, project):
        """Invoke assertions to verify correct build functionality"""
        # 1. test seed(s) should build afresh
        assert expected_fields.seed_count == test_case_fields.seed_count
        # 2. test model(s) should build afresh
        assert expected_fields.model_count == test_case_fields.model_count
        # 3. seeds should have intended row counts post update
        assert expected_fields.seed_rows == test_case_fields.seed_rows
        # 4. incremental test model(s) should be updated
        assert expected_fields.inc_test_model_count == test_case_fields.inc_test_model_count
        # 5. result table should match intended result set (itself a relation)
        check_relations_equal(
            project.adapter, [expected_fields.relation, test_case_fields.relation]
        )
