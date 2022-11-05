import pytest
import os
from tests.functional.adapter.utils.test_utils import (
    base_expected_catalog,
    expected_references_catalog,
)
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
from dbt.tests.adapter.basic.expected_catalog import no_stats
from tests.fixtures.profiles import unique_schema, dbt_profile_data

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

    # Override this fixture to allow (twin_strategy) to create a view for seeds
    # The creation of some models looks for the seed under the database/schema
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
            },
        }

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
