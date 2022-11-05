import pytest
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from tests.functional.adapter.utils.test_utils import (
    relation_from_name,
    check_relations_equal,
    check_relation_types,
)
from dbt.tests.adapter.basic.files import (
    base_view_sql,
    base_table_sql,
    base_materialized_var_sql,
)
from dbt.tests.util import (
    run_dbt,
    check_result_nodes_by_name,
)
from tests.fixtures.profiles import unique_schema, dbt_profile_data

# Unable to insert variable into docstring, so "rav-test" is hardcoded
schema_base_yml = """
version: 2
sources:
  - name: raw
    database: "rav-test" 
    schema: "{{ target.schema }}"
    tables:
      - name: seed
        identifier: "{{ var('seed_name', 'base') }}"
"""


class TestSimpleMaterializationsDremio(BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": base_view_sql,
            "table_model.sql": base_table_sql,
            "swappable.sql": base_materialized_var_sql,
            "schema.yml": schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+twin_strategy": "prevent",
            },
            "seeds": {"+twin_strategy": "allow"},
            "name": "base",
            "vars": {"dremio:reflections": "false"},
        }

    def test_base(self, project):

        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 3

        # names exist in result nodes
        check_result_nodes_by_name(results, ["view_model", "table_model", "swappable"])

        # check relation types
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation}", fetch="one"
        )
        assert result[0] == 10

        # relations_equal
        check_relations_equal(
            project.adapter, ["base", "view_model", "table_model", "swappable"]
        )

        # check relations in catalog
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 4
        assert len(catalog.sources) == 1

        # run_dbt changing materialized_var to view
        # required for BigQuery
        if project.test_config.get("require_full_refresh", False):
            results = run_dbt(
                [
                    "run",
                    "--full-refresh",
                    "-m",
                    "swappable",
                    "--vars",
                    "materialized_var: view",
                ]
            )
        else:
            results = run_dbt(
                ["run", "-m", "swappable", "--vars", "materialized_var: view"]
            )
        assert len(results) == 1
        # check relation types, swappable is view
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "view",
        }

        check_relation_types(project.adapter, expected)

        # run_dbt changing materialized_var to incremental
        results = run_dbt(
            ["run", "-m", "swappable", "--vars", "materialized_var: incremental"]
        )
        assert len(results) == 1

        # check relation types, swappable is table
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)
