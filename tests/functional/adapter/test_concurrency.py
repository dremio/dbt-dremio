import pytest
from dbt.tests.adapter.concurrency.test_concurrency import (
    BaseConcurrency,
    models__dep_sql,
    models__view_with_conflicting_cascade_sql,
    models__skip_sql,
    seeds__update_csv,
)
from dbt.tests.util import (
    check_relations_equal,
    check_table_does_not_exist,
    rm_file,
    run_dbt,
    run_dbt_and_capture,
    write_file,
)
from tests.fixtures.profiles import unique_schema, dbt_profile_data

models__invalid_sql = """
{{
  config(
    materialized = "table"
  )
}}

select a_field_that_does_not_exist from {{ ref(var('seed_name', 'seed')) }}

"""

models__table_a_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from {{ ref(var('seed_name', 'seed')) }}

"""

models__table_b_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from {{ ref(var('seed_name', 'seed')) }}

"""

models__view_model_sql = """
{{
  config(
    materialized = "view"
  )
}}

select * from {{ ref(var('seed_name', 'seed')) }}

"""


class TestConcurrency(BaseConcurrency):
    # invalid_sql, table_a_sql, table_b_sql and view_model_sql
    # are overriden to ensure the seed file path is correct.
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid.sql": models__invalid_sql,
            "table_a.sql": models__table_a_sql,
            "table_b.sql": models__table_b_sql,
            "view_model.sql": models__view_model_sql,
            "dep.sql": models__dep_sql,
            "view_with_conflicting_cascade.sql": models__view_with_conflicting_cascade_sql,
            "skip.sql": models__skip_sql,
        }

    def test_concurrency(self, project):
        run_dbt(["seed", "--select", "seed"])
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 7
        check_relations_equal(project.adapter, ["seed", "view_model"])
        check_relations_equal(project.adapter, ["seed", "dep"])
        check_relations_equal(project.adapter, ["seed", "table_a"])
        check_relations_equal(project.adapter, ["seed", "table_b"])
        check_table_does_not_exist(project.adapter, "invalid")
        check_table_does_not_exist(project.adapter, "skip")

        rm_file(project.project_root, "seeds", "seed.csv")
        write_file(seeds__update_csv, project.project_root, "seeds", "seed.csv")

        results, output = run_dbt_and_capture(["run"], expect_pass=False)
        assert len(results) == 7
        check_relations_equal(project.adapter, ["seed", "view_model"])
        check_relations_equal(project.adapter, ["seed", "dep"])
        check_relations_equal(project.adapter, ["seed", "table_a"])
        check_relations_equal(project.adapter, ["seed", "table_b"])
        check_table_does_not_exist(project.adapter, "invalid")
        check_table_does_not_exist(project.adapter, "skip")

        assert "PASS=5 WARN=0 ERROR=1 SKIP=1 TOTAL=7" in output
