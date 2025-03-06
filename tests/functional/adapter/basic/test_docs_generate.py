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
from tests.utils.util import (
    base_expected_catalog,
    expected_references_catalog,
    BUCKET,
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
    verify_catalog
)
from dbt.tests.adapter.basic.expected_catalog import no_stats

models__schema_yml = """
version: 2

models:
  - name: model
    description: "The test model"
    docs:
      show: false
    columns:
      - name: id
        description: The user ID number
        data_tests:
          - unique
          - not_null
      - name: first_name
        description: The user's first name
      - name: email
        description: The user's email
      - name: ip_address
        description: The user's IP address
      - name: updated_at
        description: The last time this user's email was updated
    data_tests:
      - test.nothing

  - name: second_model
    description: "The second test model"
    docs:
      show: false
    columns:
      - name: id
        description: The user ID number
      - name: first_name
        description: The user's first name
      - name: email
        description: The user's email
      - name: ip_address
        description: The user's IP address
      - name: updated_at
        description: The last time this user's email was updated


sources:
  - name: my_source
    description: "My source"
    loader: a_loader
    database: "{{ target.database }}"
    schema: "{{ var('test_schema') }}"
    tables:
      - name: my_table
        description: "My table"
        identifier: seed
        columns:
          - name: id
            description: "An ID field"


exposures:
  - name: simple_exposure
    type: dashboard
    depends_on:
      - ref('model')
      - source('my_source', 'my_table')
    owner:
      email: something@example.com
  - name: notebook_exposure
    type: notebook
    depends_on:
      - ref('model')
      - ref('second_model')
    owner:
      email: something@example.com
      name: Some name
    description: >
      A description of the complex exposure
    maturity: medium
    meta:
      tool: 'my_tool'
      languages:
        - python
    tags: ['my_department']
    url: http://example.com/notebook/1
"""

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

    # Override this fixture to prepend our schema with BUCKET
    # This ensures the schema works with our datalake
    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        test_file = test_file.split(".")[-1]
        unique_schema = f"{BUCKET}.{prefix}_{test_file}"
        return unique_schema

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

    # Override this fixture to set root_path=schema
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
    def models(self):
        return {
            "schema.yml": models__schema_yml,
            "second_model.sql": models__second_model_sql,
            "readme.md": models__readme_md,
            "model.sql": models__model_sql,
        }

    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        test_file = test_file.split(".")[-1]
        unique_schema = f"{BUCKET}.{prefix}_{test_file}"
        return unique_schema

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

    # Override this fixture to set root_path=schema
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

    def test_references(self, project, expected_catalog):
        start_time = run_and_generate(project)
        verify_catalog(project, expected_catalog, start_time)
