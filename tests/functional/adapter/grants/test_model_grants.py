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

import pytest, os

from dbt.tests.adapter.grants.test_model_grants import (
    BaseModelGrants,
    user2_model_schema_yml,
    user2_table_model_schema_yml,
    table_model_schema_yml,
    multiple_users_table_model_schema_yml,
    multiple_privileges_table_model_schema_yml,
)
from dbt.tests.util import (
    run_dbt_and_capture,
    get_manifest,
    write_file,
    get_connection,
)
from tests.functional.adapter.grants.base_grants import BaseGrantsDremio
from tests.utils.util import relation_from_name

role_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      grants:
        select: ["role:{{ env_var('DBT_TEST_ROLE_1') }}"]
"""

role2_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      grants:
        select: ["role:{{ env_var('DBT_TEST_ROLE_2') }}"]
"""

table_role_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select: ["role:{{ env_var('DBT_TEST_ROLE_1') }}"]
"""

role2_table_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select: ["role:{{ env_var('DBT_TEST_ROLE_2') }}"]
"""

multiple_roles_table_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select: ["role:{{ env_var('DBT_TEST_ROLE_1') }}", "role:{{ env_var('DBT_TEST_ROLE_2') }}"]
"""

multiple_privileges_role_table_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select: ["role:{{ env_var('DBT_TEST_ROLE_1') }}"]
        insert: ["role:{{ env_var('DBT_TEST_ROLE_2') }}"]
"""

users_and_roles_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      grants:
        select: ["role:{{ env_var('DBT_TEST_ROLE_1') }}", "user:{{ env_var('DBT_TEST_USER_1') }}", "user:{{ env_var('DBT_TEST_USER_2') }}"]
        insert: ["role:{{ env_var('DBT_TEST_ROLE_2') }}", "user:{{ env_var('DBT_TEST_USER_3') }}"]
"""

users_and_roles_table_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select: ["role:{{ env_var('DBT_TEST_ROLE_1') }}", "user:{{ env_var('DBT_TEST_USER_1') }}", "user:{{ env_var('DBT_TEST_USER_2') }}"]
        insert: ["role:{{ env_var('DBT_TEST_ROLE_2') }}", "user:{{ env_var('DBT_TEST_USER_3') }}"]
"""

@pytest.mark.skip(reason="Dremio only supports grants in EE/DC editions.")
class TestViewGrantsDremio(BaseGrantsDremio, BaseModelGrants):
    def get_test_roles(self):
        test_roles = []
        for env_var in ["DBT_TEST_ROLE_1", "DBT_TEST_ROLE_2"]:
            role_name = os.getenv(env_var)
            if role_name:
                test_roles.append(role_name)
        return test_roles

    # Overridden to only include view materialization
    def test_view_table_grants(self, project, get_test_users):
        # we want the test to fail, not silently skip
        test_users = get_test_users
        test_roles = self.get_test_roles()
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]
        assert len(test_users) == 3

        # View materialization, single select grant
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        model = manifest.nodes[model_id]
        expected = {select_privilege_name: [test_users[0]]}
        assert model.config.grants == expected
        assert model.config.materialized == "view"
        expected = {select_privilege_name: ["user:" + test_users[0]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        # View materialization, change select grant user
        updated_yaml = self.interpolate_name_overrides(user2_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        expected = {select_privilege_name: ["user:" + get_test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        # roles
        updated_yaml = self.interpolate_name_overrides(role_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        expected = {select_privilege_name: ["role:" + test_roles[0]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        updated_yaml = self.interpolate_name_overrides(role2_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        expected = {select_privilege_name: ["role:" + test_roles[1]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        updated_yaml = self.interpolate_name_overrides(users_and_roles_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        insert_privilege_name = self.privilege_grantee_name_overrides()["insert"]
        expected = {
            select_privilege_name: ["role:" + test_roles[0], "user:" + get_test_users[0],
                                    "user:" + get_test_users[1]],
            insert_privilege_name: ["role:" + test_roles[1], "user:" + get_test_users[2]],
        }
        self.assert_expected_grants_match_actual(project, "my_model", expected)


@pytest.mark.skip(reason="Dremio only supports grants in EE/DC editions.")
class TestTableGrantsDremio(BaseGrantsDremio, BaseModelGrants):
    # Need to override this to make sure it uses our modified version of relation_from_name
    # This isn't needed for views, as dbt-core's version defaults to database/schema path
    def get_grants_on_relation(self, project, relation_name):
        relation = relation_from_name(project.adapter, relation_name)
        adapter = project.adapter
        with get_connection(adapter):
            kwargs = {"relation": relation}
            show_grant_sql = adapter.execute_macro("get_show_grant_sql", kwargs=kwargs)
            _, grant_table = adapter.execute(show_grant_sql, fetch=True)
            actual_grants = adapter.standardize_grants_dict(grant_table)
        return actual_grants

    def get_test_roles(self):
        test_roles = []
        for env_var in ["DBT_TEST_ROLE_1", "DBT_TEST_ROLE_2"]:
            role_name = os.getenv(env_var)
            if role_name:
                test_roles.append(role_name)
        return test_roles

    # Overridden to only include table materializations
    def test_view_table_grants(self, project, get_test_users):
        test_users = get_test_users
        test_roles = self.get_test_roles()
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]
        insert_privilege_name = self.privilege_grantee_name_overrides()["insert"]
        assert len(test_users) == 3
        # Table materialization, single select grant
        updated_yaml = self.interpolate_name_overrides(table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        model = manifest.nodes[model_id]
        assert model.config.materialized == "table"
        expected = {select_privilege_name: ["user:" + test_users[0]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        # Table materialization, change select grant user
        updated_yaml = self.interpolate_name_overrides(user2_table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "table"
        expected = {select_privilege_name: ["user:" + test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        # Table materialization, multiple grantees
        updated_yaml = self.interpolate_name_overrides(
            multiple_users_table_model_schema_yml
        )
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "table"
        expected = {select_privilege_name: ["user:" + test_users[0], "user:" + test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        # Table materialization, multiple privileges
        updated_yaml = self.interpolate_name_overrides(
            multiple_privileges_table_model_schema_yml
        )
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "table"
        expected = {
            select_privilege_name: ["user:" + test_users[0]],
            insert_privilege_name: ["user:" + test_users[1]],
        }
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        # roles
        updated_yaml = self.interpolate_name_overrides(table_role_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        expected = {select_privilege_name: ["role:" + test_roles[0]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        updated_yaml = self.interpolate_name_overrides(role2_table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        expected = {select_privilege_name: ["role:" + test_roles[1]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        updated_yaml = self.interpolate_name_overrides(multiple_roles_table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        expected = {select_privilege_name: ["role:" + test_roles[0], "role:" + test_roles[1]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        updated_yaml = self.interpolate_name_overrides(multiple_privileges_role_table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        expected = {
            select_privilege_name: ["role:" + test_roles[0]],
            insert_privilege_name: ["role:" + test_roles[1]],
        }
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        updated_yaml = self.interpolate_name_overrides(users_and_roles_table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        expected = {
            select_privilege_name: ["role:" + test_roles[0], "user:" + get_test_users[0], "user:" + get_test_users[1]],
            insert_privilege_name: ["role:" + test_roles[1], "user:" + get_test_users[2]],
        }
        self.assert_expected_grants_match_actual(project, "my_model", expected)
