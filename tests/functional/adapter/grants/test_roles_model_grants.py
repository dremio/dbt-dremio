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
    run_dbt,
    get_manifest,
    write_file,
    get_connection,
    run_dbt_and_capture
)
from tests.functional.adapter.grants.base_grants import BaseGrantsDremio
from tests.utils.util import relation_from_name

TEST_ROLE_ENV_VARS = ["DBT_TEST_ROLE_1", "DBT_TEST_ROLE_2"]

my_model_sql = """
  select 1 as fun
"""

role1_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      grants:
        select: ["role:{{ env_var('DBT_TEST_ROLE_1') }}"]
"""

multiple_roles_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      grants:
        select: ["role:{{ env_var('DBT_TEST_ROLE_1') }}", "role:{{ env_var('DBT_TEST_ROLE_2') }}"]
"""

users_and_roles_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      grants:
        select: ["user:{{ env_var('DBT_TEST_USER_1') }}", "user:{{ env_var('DBT_TEST_USER_2') }}", "role:{{ env_var('DBT_TEST_ROLE_1') }}", "role:{{ env_var('DBT_TEST_ROLE_2') }}"]
"""

class TestViewRolesGrantsDremio(BaseGrantsDremio, BaseModelGrants):
    @pytest.fixture(scope="class", autouse=True)
    def get_test_roles(self, project):
        test_roles = []
        for env_var in TEST_ROLE_ENV_VARS:
            role = os.getenv(env_var)
            if role:
                test_roles.append(role)
        return test_roles

    def test_view_role1_grants(self, project, get_test_roles):
        test_roles = get_test_roles
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]
        assert len(test_roles) == 2

        # View materialization, single select grant
        updated_yaml = self.interpolate_name_overrides(role1_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        model = manifest.nodes[model_id]
        assert model.config.materialized == "view"
        expected = {select_privilege_name: ["role:" + test_roles[0]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

    def test_view_multiple_roles_grants(self, project, get_test_roles):
        test_roles = get_test_roles
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]
        assert len(test_roles) == 2

        # View materialization, single select grant
        updated_yaml = self.interpolate_name_overrides(multiple_roles_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        model = manifest.nodes[model_id]
        assert model.config.materialized == "view"
        expected = {select_privilege_name: ["role:" + test_roles[0], "role:" + test_roles[1]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

    def test_view_multiple_users_and_roles(self, project, get_test_users, get_test_roles):
        test_users = get_test_users
        assert len(test_users) == 3
        test_roles = get_test_roles
        assert len(test_roles) == 2

        select_privilege_name = self.privilege_grantee_name_overrides()["select"]

        # View materialization, multiple select grants
        updated_yaml = self.interpolate_name_overrides(users_and_roles_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        model = manifest.nodes[model_id]
        assert model.config.materialized == "view"
        expected = {select_privilege_name: ["user:" + test_users[0], "user:" + test_users[1], "role:" + test_roles[0], "role:" + test_roles[1]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)