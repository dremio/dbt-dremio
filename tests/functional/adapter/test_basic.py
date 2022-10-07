import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.files import base_view_sql, base_table_sql, base_materialized_var_sql, seeds_base_csv

from dbt.tests.util import (
    run_dbt,
    check_result_nodes_by_name,
    relation_from_name,
    check_relation_types,
    check_relations_equal,
)
SCHEMA = "rav-test"

schema_base_yml = """
version: 2
sources:
  - name: raw
    schema: "rav-test"
    database: "rav-test"
    tables:
      - name: seed
        identifier: "{{ var('seed_name', 'hola') }}"
"""


class TestSimpleMaterializationsDremio(BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "hola.csv": seeds_base_csv,
        }

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
                "+twin_strategy": "allow",
            },
            "seeds": {
                "+twin_strategy": "allow"
            }
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
        check_result_nodes_by_name(
            results, ["view_model", "table_model", "swappable"])

        # check relation types
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)

        # base table rowcount
        relation = relation_from_name(
            project.adapter, f"rav-test.{SCHEMA}.hola")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # relations_equal
        check_relations_equal(
            project.adapter, ["base", "view_model", "table_model", "swappable"])

        # check relations in catalog
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 4
        assert len(catalog.sources) == 1

        # run_dbt changing materialized_var to view
        # required for BigQuery
        if project.test_config.get("require_full_refresh", False):
            results = run_dbt(
                ["run", "--full-refresh", "-m", "swappable",
                    "--vars", "materialized_var: view"]
            )
        else:
            results = run_dbt(
                ["run", "-m", "swappable", "--vars", "materialized_var: view"])
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
        results = run_dbt(["run", "-m", "swappable", "--vars",
                          "materialized_var: incremental"])
        assert len(results) == 1

        # check relation types, swappable is table
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)


class TestSingularTestsDremio(BaseSingularTests):
    pass


class TestSingularTestsEphemeralDremio(BaseSingularTestsEphemeral):
    pass


class TestEmptyDremio(BaseEmpty):
    pass


class TestEphemeralDremio(BaseEphemeral):
    pass


class TestIncrementalDremio(BaseIncremental):
    pass


class TestGenericTestsDremio(BaseGenericTests):
    pass


class TestSnapshotCheckColsDremio(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampDremio(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethodDremio(BaseAdapterMethod):
    pass
