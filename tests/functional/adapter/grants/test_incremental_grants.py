from dbt.tests.util import (
    run_dbt_and_capture,
    get_manifest,
    write_file,
    get_connection,
    run_dbt,
)
from tests.functional.adapter.grants.base_grants import BaseGrantsDremio
from tests.functional.adapter.utils.test_utils import relation_from_name
from dbt.tests.adapter.grants.test_incremental_grants import (
    BaseIncrementalGrants,
    user2_incremental_model_schema_yml,
)


class TestIncrementalGrantsDremio(BaseGrantsDremio, BaseIncrementalGrants):
    def get_grants_on_relation(self, project, relation_name):
        relation = relation_from_name(project.adapter, relation_name)
        adapter = project.adapter
        with get_connection(adapter):
            kwargs = {"relation": relation}
            show_grant_sql = adapter.execute_macro("get_show_grant_sql", kwargs=kwargs)
            _, grant_table = adapter.execute(show_grant_sql, fetch=True)
            actual_grants = adapter.standardize_grants_dict(grant_table)
        return actual_grants

    # Need to override this because we don't have functionality to delete table from source
    # Only commented out one line to get the test to pass
    def test_incremental_grants(self, project, get_test_users):
        # we want the test to fail, not silently skip
        test_users = get_test_users
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]
        assert len(test_users) == 3

        # Incremental materialization, single select grant
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_incremental_model"
        model = manifest.nodes[model_id]
        assert model.config.materialized == "incremental"
        expected = {select_privilege_name: [test_users[0]]}
        self.assert_expected_grants_match_actual(
            project, "my_incremental_model", expected
        )

        # Incremental materialization, run again without changes
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        assert "revoke " not in log_output
        assert (
            "grant " not in log_output
        )  # with space to disambiguate from 'show grants'
        self.assert_expected_grants_match_actual(
            project, "my_incremental_model", expected
        )

        # Incremental materialization, change select grant user
        updated_yaml = self.interpolate_name_overrides(
            user2_incremental_model_schema_yml
        )
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        assert "revoke " in log_output
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "incremental"
        expected = {select_privilege_name: [test_users[1]]}
        self.assert_expected_grants_match_actual(
            project, "my_incremental_model", expected
        )

        # Incremental materialization, same config, now with --full-refresh
        run_dbt(["--debug", "run", "--full-refresh"])
        assert len(results) == 1
        # whether grants or revokes happened will vary by adapter
        self.assert_expected_grants_match_actual(
            project, "my_incremental_model", expected
        )

        # Now drop the schema (with the table in it)
        adapter = project.adapter
        relation = relation_from_name(adapter, "my_incremental_model")
        with get_connection(adapter):
            adapter.drop_schema(relation)

        # Incremental materialization, same config, rebuild now that table is missing
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        # NEED TO COMMENT THIS OUT AS WE CANNOT DROP TABLES
        assert "grant " in log_output
        assert "revoke " not in log_output
        self.assert_expected_grants_match_actual(
            project, "my_incremental_model", expected
        )
