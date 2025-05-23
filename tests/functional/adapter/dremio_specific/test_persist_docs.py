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
from dbt.tests.adapter.persist_docs.test_persist_docs import BasePersistDocs
from dbt.tests.adapter.persist_docs.fixtures import (
    _DOCS__MY_FUN_DOCS,
    _MODELS__TABLE,
    _MODELS__VIEW
)
from dbt.tests.util import run_dbt, write_file

from dbt.adapters.dremio.api.parameters import ParametersBuilder

from build.lib.dbt.adapters.dremio.api.rest.client import DremioRestClient

from tests.utils.util import BUCKET, SOURCE

# Excluded seed
_PROPERTIES__SCHEMA_YML = """
version: 2
models:
  - name: table_model
    description: |
      Table model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      80% of statistics are made up on the spot
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          80% of statistics are made up on the spot
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
      - name: name
        description: |
          Some stuff here and then a call to
          {{ doc('my_fun_doc')}}
    config:
        tags: ["test_tag1", "test_tag2", "test_tag3"]
  - name: view_model
    description: |
      View model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      80% of statistics are made up on the spot
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          80% of statistics are made up on the spot
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    config:
        tags: "test_tag"
"""

_PROPERTIES__UPDATING_VIEW_SCHEMA_YML = """
version: 2
models:
  - name: table_model
    description: |
      Table model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      80% of statistics are made up on the spot
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          80% of statistics are made up on the spot
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
      - name: name
        description: |
          Some stuff here and then a call to
          {{ doc('my_fun_doc')}}
    config:
        tags: ["test_tag1", "test_tag2", "test_tag3"]
  - name: view_model
    description: "Updated view description!"
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          80% of statistics are made up on the spot
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    config:
        tags: ["test_tag","new_tag"]
"""

_PROPERTIES__DELETING_VIEW_SCHEMA_YML = """
version: 2
models:
  - name: table_model
    description: |
      Table model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      80% of statistics are made up on the spot
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          80% of statistics are made up on the spot
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
      - name: name
        description: |
          Some stuff here and then a call to
          {{ doc('my_fun_doc')}}
    config:
        tags: ["test_tag1", "test_tag2", "test_tag3"]
  - name: view_model
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          80% of statistics are made up on the spot
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
"""

class TestPersistDocs(BasePersistDocs):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": True
                    },
                }
            }
        }
    
    # This ensures the schema works with our datalake
    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        # We only want the last part of the name
        test_file = test_file.split(".")[-1]
        unique_schema = f"{BUCKET}.{prefix}_{test_file}"
        return unique_schema

    # Override this fixture to set root_path=schema
    @pytest.fixture(scope="class")
    def dbt_profile_data(self, unique_schema, dbt_profile_target, profiles_config_update):
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
        target["root_path"] = f"{unique_schema}"
        profile["test"]["outputs"]["default"] = target

        if profiles_config_update:
            profile.update(profiles_config_update)
        return profile
    
    # Overriding this fixture and setting autouse to be False so we are able to perform
    # run_dbt accordingly in each of the following tests
    @pytest.fixture(scope="class", autouse=False)
    def setUp(self, project):
        pass

    @pytest.fixture(scope="class")
    def client(self, adapter):
        credentials = adapter.connections.profile.credentials
        parameters = ParametersBuilder.build(credentials)
        client = DremioRestClient(parameters.get_parameters())

        return client
    
    # Removing unnecessary models and adding schema
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": _MODELS__TABLE,
            "view_model.sql": _MODELS__VIEW,
            "schema.yml": _PROPERTIES__SCHEMA_YML,
        }
    
    # Removing schema from properties
    @pytest.fixture(scope="class")
    def properties(self):
        return {
            "my_fun_docs.md": _DOCS__MY_FUN_DOCS,
        }
    
    def _create_path_list(self, database, schema):
        path = [database]
        if schema != 'no_schema':
            folders = schema.split(".")
            path.extend(folders)
        return path
    
    def _get_relation_id(self, project, client, identifier):
        client.start()

        if identifier == "table_model":
            database = SOURCE 
        else:
            database = project.database
        schema = project.test_schema
        path = self._create_path_list(database, schema)

        path.append(identifier)

        catalog_info = client.get_catalog_item(
            catalog_id=None,
            catalog_path=path,
        )
        return catalog_info.get("id")
    
    # Overriding the original test, to be ignored because it was testing 
    # the original persist_docs behavior, which does not apply anymore
    def test_has_comments_pglike(self, project):
        pass

    def test_table_model_create_wikis_and_tags(self, project, client):
        run_dbt(["run", "--select", "table_model"])
        object_id = self._get_relation_id(project, client, "table_model")
        wiki = client.retrieve_wiki(object_id)
        tags = client.retrieve_tags(object_id)
        self._assert_table_wikis_and_tags(wiki, tags)

    def test_view_model_create_wikis_and_tags(self, project, client):
        # Create + Get
        run_dbt(["run", "--select", "view_model"])
        object_id = self._get_relation_id(project, client, "view_model")
        wiki = client.retrieve_wiki(object_id)
        tags = client.retrieve_tags(object_id)
        self._assert_view_wikis_and_tags(wiki, tags)

    def test_view_model_wikis_and_tags_remain_when_no_changes(self, project, client):
        # No changes in wikis / tags , version should be the same
        run_dbt(["run", "--select", "view_model"])
        object_id = self._get_relation_id(project, client, "view_model")
        wiki = client.retrieve_wiki(object_id)
        tags = client.retrieve_tags(object_id)
        self._assert_view_wikis_and_tags(wiki, tags)

    def test_view_model_update_wikis_and_tags(self, project, client):
        # Previous tags
        object_id = self._get_relation_id(project, client, "view_model")
        tags = client.retrieve_tags(object_id)
        # Update
        write_file(_PROPERTIES__UPDATING_VIEW_SCHEMA_YML, project.project_root, "models", "schema.yml")
        run_dbt(["run", "--select", "view_model"])
        object_id = self._get_relation_id(project, client, "view_model")
        updated_wiki = client.retrieve_wiki(object_id)
        updated_tags = client.retrieve_tags(object_id)
        self._assert_view_wikis_and_tags_update(updated_wiki, updated_tags, tags["version"])

    def test_view_model_delete_wikis_and_tags(self, project, client):
        # Previous tags
        object_id = self._get_relation_id(project, client, "view_model")
        tags = client.retrieve_tags(object_id)
        # Delete
        write_file(_PROPERTIES__DELETING_VIEW_SCHEMA_YML, project.project_root, "models", "schema.yml")
        run_dbt(["run", "--select", "view_model"])
        object_id = self._get_relation_id(project, client, "view_model")
        deleted_wiki = client.retrieve_wiki(object_id)
        deleted_tags = client.retrieve_tags(object_id)
        self._assert_view_wikis_and_tags_delete(deleted_wiki, deleted_tags, tags["version"])

    def _assert_table_wikis_and_tags(self, wiki, tags):
        expected_wiki = """Table model description "with double quotes"
and with 'single  quotes' as welll as other;
'''abc123'''
reserved -- characters
80% of statistics are made up on the spot
--
/* comment */
Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
"""
        expected_tags = ["test_tag1", "test_tag2", "test_tag3"]
        assert wiki.get("text") == expected_wiki and wiki.get("version") == 0
        assert tags.get("tags") == expected_tags

    def _assert_view_wikis_and_tags(self, wiki, tags):
        expected_wiki = """View model description "with double quotes"
and with 'single  quotes' as welll as other;
'''abc123'''
reserved -- characters
80% of statistics are made up on the spot
--
/* comment */
Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
"""
        expected_tags = ["test_tag"]
        assert wiki.get("text") == expected_wiki and wiki.get("version") == 0
        assert tags.get("tags") == expected_tags

    def _assert_view_wikis_and_tags_update(self, wiki, tags, previous_tag_version):
        expected_wiki = "Updated view description!"
        expected_tags = ["test_tag","new_tag"]
        assert wiki.get("text") == expected_wiki and wiki.get("version") == 1
        assert tags.get("tags") == expected_tags and tags.get("version") != previous_tag_version

    def _assert_view_wikis_and_tags_delete(self, wiki, tags, previous_tag_version):
        assert wiki.get("text") == "" and wiki.get("version") == 2
        assert tags.get("tags") == [] and tags.get("version") != previous_tag_version
