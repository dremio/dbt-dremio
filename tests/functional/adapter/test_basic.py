import pytest
import os
import time
from tests.functional.adapter.utils.test_utils import (
    relation_from_name,
    check_relations_equal,
    check_relation_types,
    base_expected_catalog,
    expected_references_catalog,
)
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_docs_generate import (
    BaseDocsGenerate,
    BaseDocsGenReferences,
    verify_metadata,
    models__readme_md,
    models__model_sql,
    models__schema_yml,
    run_and_generate,
    get_artifact,
)
from dbt.tests.adapter.basic.files import (
    base_view_sql,
    base_table_sql,
    base_materialized_var_sql,
)
from dbt.tests.adapter.basic.test_adapter_methods import models__upstream_sql

from dbt.tests.util import (
    run_dbt,
    check_result_nodes_by_name,
)
from dbt.events import AdapterLogger
from dbt.tests.adapter.basic.expected_catalog import no_stats

logger = AdapterLogger("dremio")

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

# Login endpoint is being hit too many times
@pytest.fixture(autouse=True)
def throttle_login_connections():
    yield
    time.sleep(1)


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

    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        # We only want the last part of the name
        test_file = test_file.split(".")[-1]
        unique_schema = f"rav-test.{prefix}_{test_file}"
        return unique_schema

    @pytest.fixture(scope="class")
    def dbt_profile_data(
        self, unique_schema, dbt_profile_target, profiles_config_update
    ):
        profile = {
            "config": {"send_anonymous_usage_stats": False},
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


class TestSingularTestsDremio(BaseSingularTests):
    def test_singular_tests(self, project):
        # test command
        results = run_dbt(["test"], expect_pass=False)
        assert len(results) == 2

        # We have the right result nodes
        check_result_nodes_by_name(results, ["passing", "failing"])

        # Check result status
        for result in results:
            if result.node.name == "passing":
                assert result.status == "pass"
            elif result.node.name == "failing":
                assert result.status == "fail"


class TestSingularTestsEphemeralDremio(BaseSingularTestsEphemeral):
    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        # We only want the last part of the name
        test_file = test_file.split(".")[-1]
        unique_schema = f"rav-test.{prefix}_{test_file}"
        return unique_schema

    @pytest.fixture(scope="class")
    def dbt_profile_data(
        self, unique_schema, dbt_profile_target, profiles_config_update
    ):
        profile = {
            "config": {"send_anonymous_usage_stats": False},
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


class TestEmptyDremio(BaseEmpty):
    pass


class TestEphemeralDremio(BaseEphemeral):
    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        # We only want the last part of the name
        test_file = test_file.split(".")[-1]
        unique_schema = f"rav-test.{prefix}_{test_file}"
        return unique_schema

    @pytest.fixture(scope="class")
    def dbt_profile_data(
        self, unique_schema, dbt_profile_target, profiles_config_update
    ):
        profile = {
            "config": {"send_anonymous_usage_stats": False},
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


class TestIncrementalDremio(BaseIncremental):
    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        # We only want the last part of the name
        test_file = test_file.split(".")[-1]
        unique_schema = f"rav-test.{prefix}_{test_file}"
        return unique_schema

    @pytest.fixture(scope="class")
    def dbt_profile_data(
        self, unique_schema, dbt_profile_target, profiles_config_update
    ):
        profile = {
            "config": {"send_anonymous_usage_stats": False},
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


class TestGenericTestsDremio(BaseGenericTests):
    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        # We only want the last part of the name
        test_file = test_file.split(".")[-1]
        unique_schema = f"rav-test.{prefix}_{test_file}"
        return unique_schema

    @pytest.fixture(scope="class")
    def dbt_profile_data(
        self, unique_schema, dbt_profile_target, profiles_config_update
    ):
        profile = {
            "config": {"send_anonymous_usage_stats": False},
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


@pytest.mark.skip(reason="https://github.com/dremio/dbt-dremio/issues/20")
class TestSnapshotCheckColsDremio(BaseSnapshotCheckCols):
    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        # We only want the last part of the name
        test_file = test_file.split(".")[-1]
        unique_schema = f"rav-test.{prefix}_{test_file}"
        return unique_schema

    @pytest.fixture(scope="class")
    def dbt_profile_data(
        self, unique_schema, dbt_profile_target, profiles_config_update
    ):
        profile = {
            "config": {"send_anonymous_usage_stats": False},
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
        target["database"] = target["datalake"]
        profile["test"]["outputs"]["default"] = target

        if profiles_config_update:
            profile.update(profiles_config_update)
        return profile

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {"+twin_strategy": "prevent"},
            "name": "snapshot_strategy_check_cols",
        }


class TestSnapshotTimestampDremio(BaseSnapshotTimestamp):
    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        # We only want the last part of the name
        test_file = test_file.split(".")[-1]
        unique_schema = f"rav-test.{prefix}_{test_file}"
        return unique_schema

    @pytest.fixture(scope="class")
    def dbt_profile_data(
        self, unique_schema, dbt_profile_target, profiles_config_update
    ):
        profile = {
            "config": {"send_anonymous_usage_stats": False},
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
        target["database"] = target["datalake"]
        profile["test"]["outputs"]["default"] = target

        if profiles_config_update:
            profile.update(profiles_config_update)
        return profile

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {"+twin_strategy": "prevent"},
            "name": "snapshot_strategy_timestamp",
        }


models__my_model_sql = """

{% set upstream = ref('upstream_view') %}

{% if execute %}
    {# don't ever do any of this #}
    {%- do adapter.drop_schema(upstream) -%}
    {% set existing = adapter.get_relation(upstream.database, upstream.schema, upstream.identifier) %}
    {% if existing is not defined %}
        {% do exceptions.raise_compiler_error('expected ' ~ ' to not exist, but it did') %}
    {% endif %}

    {%- do adapter.create_schema(upstream) -%}

    {% set sql = create_view_as(upstream, 'select 2 as id') %}
    {% do run_query(sql) %}
{% endif %}


select * from {{ upstream }}

"""

models__expected_sql = """
-- make sure this runs after 'model'
-- {{ ref('model_view') }}
select 2 as id

"""


class TestBaseAdapterMethodDremio(BaseAdapterMethod):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+twin_strategy": "clone",
            },
            "name": "adapter_methods",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream_view.sql": models__upstream_sql,
            "expected_view.sql": models__expected_sql,
            "model_view.sql": models__my_model_sql,
        }

    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        # We only want the last part of the name
        test_file = test_file.split(".")[-1]
        unique_schema = f"rav-test.{prefix}_{test_file}"
        return unique_schema

    @pytest.fixture(scope="class")
    def dbt_profile_data(
        self, unique_schema, dbt_profile_target, profiles_config_update
    ):
        profile = {
            "config": {"send_anonymous_usage_stats": False},
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
    def equal_tables(self):
        return ["model_view", "expected_view"]


# required to explicitly use alternate_schema
# otherwise will use unique_schema under profiles fixture
models__second_model_sql = """
{{
    config(
        materialized='view',
        schema=var('alternate_schema')
    )
}}

select * from {{ ref('seed') }}
"""

# Remove check for sources and only include nodes
def verify_catalog_nodes(project, expected_catalog, start_time):
    # get the catalog.json
    catalog_path = os.path.join(project.project_root, "target", "catalog.json")
    assert os.path.exists(catalog_path)
    catalog = get_artifact(catalog_path)

    # verify the catalog
    assert set(catalog) == {"errors", "metadata", "nodes", "sources"}
    verify_metadata(
        catalog["metadata"],
        "https://schemas.getdbt.com/dbt/catalog/v1.json",
        start_time,
    )
    assert not catalog["errors"]
    key = "nodes"
    for unique_id, expected_node in expected_catalog[key].items():
        found_node = catalog[key][unique_id]
        for node_key in expected_node:
            assert node_key in found_node
            assert (
                found_node[node_key] == expected_node[node_key]
            ), f"Key '{node_key}' in '{unique_id}' did not match"


class TestBaseDocsGenerateDremio(BaseDocsGenerate):
    # Override this fixture to add our version of second_model
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models__schema_yml,
            "second_model.sql": models__second_model_sql,
            "readme.md": models__readme_md,
            "model.sql": models__model_sql,
        }

    # Override this fixture to prepend our schema with rav-test
    # This ensures the schema works with our datalake
    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        test_file = test_file.split(".")[-1]
        unique_schema = f"rav-test.{prefix}_{test_file}"
        return unique_schema

    # Override this fixture to prevent (twin_strategy) creating a view for seeds
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        alternate_schema = unique_schema + "_test"
        return {
            "asset-paths": ["assets", "invalid-asset-paths"],
            "vars": {
                "test_schema": unique_schema,
                "alternate_schema": alternate_schema,
            },
            "seeds": {
                "quote_columns": True,
                "+twin_strategy": "prevent",
            },
        }

    # Override this fixture to set root_path=schema
    @pytest.fixture(scope="class")
    def dbt_profile_data(
        self, unique_schema, dbt_profile_target, profiles_config_update
    ):
        profile = {
            "config": {"send_anonymous_usage_stats": False},
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

    # Override this fixture to change expected types to Dremio types
    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return base_expected_catalog(
            project,
            role=None,
            id_type="bigint",
            text_type="character varying",
            time_type="timestamp",
            view_type="view",
            table_type="table",
            model_stats=no_stats(),
        )

    # Test "--no-compile" flag works and produces no manifest.json
    def test_run_and_generate_no_compile(self, project, expected_catalog):
        start_time = run_and_generate(project, ["--no-compile"])
        assert not os.path.exists(
            os.path.join(project.project_root, "target", "manifest.json")
        )
        verify_catalog_nodes(project, expected_catalog, start_time)

    # Test generic "docs generate" command
    def test_run_and_generate(self, project, expected_catalog):
        start_time = run_and_generate(project)
        verify_catalog_nodes(project, expected_catalog, start_time)

        # Check that assets have been copied to the target directory for use in the docs html page
        assert os.path.exists(os.path.join(".", "target", "assets"))
        assert os.path.exists(os.path.join(".", "target", "assets", "lorem-ipsum.txt"))
        assert not os.path.exists(os.path.join(".", "target", "non-existent-assets"))


class TestBaseDocsGenReferencesDremio(BaseDocsGenReferences):
    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        test_file = test_file.split(".")[-1]
        unique_schema = f"rav-test.{prefix}_{test_file}"
        return unique_schema

    # Override this fixture to prevent (twin_strategy) creating a view for seeds
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        alternate_schema = unique_schema + "_test"
        return {
            "asset-paths": ["assets", "invalid-asset-paths"],
            "vars": {
                "test_schema": unique_schema,
                "alternate_schema": alternate_schema,
            },
            "seeds": {
                "quote_columns": True,
                "+twin_strategy": "clone",
            },
            "models": {
                "+twin_strategy": "prevent",
            },
        }

    # Override this fixture to set root_path=schema
    @pytest.fixture(scope="class")
    def dbt_profile_data(
        self, unique_schema, dbt_profile_target, profiles_config_update
    ):
        profile = {
            "config": {"send_anonymous_usage_stats": False},
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

    # Override this fixture to change expected types to Dremio types
    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return expected_references_catalog(
            project,
            role=None,
            id_type="bigint",
            text_type="character varying",
            time_type="timestamp",
            view_type="view",
            table_type="table",
            model_stats=no_stats(),
            bigint_type="bigint",
        )
