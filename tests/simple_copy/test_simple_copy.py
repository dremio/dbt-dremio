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

# mix in biguery
# mix in snowflake

import pytest

from pathlib import Path

from tests.utils.util import check_relations_equal
from dbt.tests.util import run_dbt, rm_file, write_file

from dbt.tests.adapter.simple_copy.fixtures import (
    _PROPERTIES__SCHEMA_YML,
    _SEEDS__SEED_INITIAL,
    _SEEDS__SEED_UPDATE,
    _MODELS__ADVANCED_INCREMENTAL,
    _MODELS__COMPOUND_SORT,
    _MODELS__DISABLED,
    _MODELS__EMPTY,
    _MODELS__GET_AND_REF,
    _MODELS__INCREMENTAL,
    _MODELS__INTERLEAVED_SORT,
    _MODELS__MATERIALIZED,
    _MODELS__VIEW_MODEL,
)

from tests.fixtures.profiles import unique_schema, dbt_profile_data
from tests.utils.util import BUCKET, SOURCE


class SimpleCopySetup:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "advanced_incremental.sql": _MODELS__ADVANCED_INCREMENTAL,
            "compound_sort.sql": _MODELS__COMPOUND_SORT,
            "disabled.sql": _MODELS__DISABLED,
            "empty.sql": _MODELS__EMPTY,
            "get_and_ref_view.sql": _MODELS__GET_AND_REF,
            "incremental.sql": _MODELS__INCREMENTAL,
            "interleaved_sort.sql": _MODELS__INTERLEAVED_SORT,
            "materialized.sql": _MODELS__MATERIALIZED,
            "view_model.sql": _MODELS__VIEW_MODEL,
        }

    @pytest.fixture(scope="class")
    def properties(self):
        return {
            "schema.yml": _PROPERTIES__SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": _SEEDS__SEED_INITIAL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"quote_columns": False}}


class SimpleCopyBase(SimpleCopySetup):
    def test_simple_copy(self, project):
        # Load the seed file and check that it worked
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Run the project and ensure that all the models loaded
        results = run_dbt()
        assert len(results) == 7
        check_relations_equal(
            project.adapter,
            ["seed", "view_model", "incremental", "materialized", "get_and_ref_view"],
        )

        # Change the seed.csv file and see if everything is the same, i.e. everything has been updated
        main_seed_file = project.project_root / Path("seeds") / Path("seed.csv")
        rm_file(main_seed_file)
        write_file(_SEEDS__SEED_UPDATE, main_seed_file)
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 7
        check_relations_equal(
            project.adapter,
            ["seed", "view_model", "incremental", "materialized", "get_and_ref_view"],
        )

    def test_simple_copy_with_materialized_views(self, project):
        project.run_sql(
            f"create table {SOURCE}.{BUCKET}.{project.test_schema}.unrelated_table (id int)"
        )
        sql = f"""
            create view "dbt_test".{project.test_schema}.unrelated_materialized_view as (
                select * from {SOURCE}.{BUCKET}.{project.test_schema}.unrelated_table
            )
        """
        project.run_sql(sql)
        sql = f"""
            create view "dbt_test".{project.test_schema}.unrelated_view as (
                select * from "dbt_test".{project.test_schema}.unrelated_materialized_view
            )
        """
        project.run_sql(sql)
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 7
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

def _dremio_get_tables_in_schema(self):
    sql = """
                select table_name,
                        case when table_type = 'BASE TABLE' then 'table'
                             when table_type = 'VIEW' then 'view'
                             else table_type
                        end as materialization
                from information_schema."tables"
                where {}
                order by table_name
                """
    sql = sql.format("{} LIKE '{}'".format("table_schema", self.test_schema))
    result = self.run_sql(sql, fetch="all")
    return {model_name: materialization for (model_name, materialization) in result}


class EmptyModelsArentRunBase(SimpleCopySetup):
    def test_dbt_doesnt_run_empty_models(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 7

        tables = _dremio_get_tables_in_schema(self=project)

        assert "empty" not in tables.keys()
        assert "disabled" not in tables.keys()


class TestSimpleCopyBaseDremio(SimpleCopyBase):
    pass


class TestEmptyModelsArentRunDremio(EmptyModelsArentRunBase):
    pass
