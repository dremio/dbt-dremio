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

from dbt.tests.adapter.grants.test_seed_grants import (
    BaseSeedGrants,
    user2_schema_base_yml,
    ignore_grants_yml,
    zero_grants_yml,
)
from tests.functional.adapter.grants.base_grants import BaseGrantsDremio
from tests.utils.util import relation_from_name
from dbt.tests.util import (
    get_connection,
    get_manifest,
    run_dbt,
    run_dbt_and_capture,
    write_file,
)

DREMIO_EDITION = os.getenv("DREMIO_EDITION")

@pytest.mark.skipif(DREMIO_EDITION == "community", reason="Dremio only supports grants in EE/DC editions.")
class TestSeedGrantsDremio(BaseGrantsDremio, BaseSeedGrants):

    # Grants are reapplied if dbt run is ran twice without changes
    def seeds_support_partial_refresh(self):
        return False

    # Overrride this to use our version of relation_from_name
    def get_grants_on_relation(self, project, relation_name):
        relation = relation_from_name(project.adapter, relation_name)
        adapter = project.adapter
        with get_connection(adapter):
            kwargs = {"relation": relation}
            show_grant_sql = adapter.execute_macro("get_show_grant_sql", kwargs=kwargs)
            _, grant_table = adapter.execute(show_grant_sql, fetch=True)
            actual_grants = adapter.standardize_grants_dict(grant_table)
        return actual_grants

    # Override to add user prefix in expected results
    def test_seed_grants(self, project, get_test_users):
        test_users = get_test_users
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]

        # seed command
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        expected = {select_privilege_name: ["user:" + test_users[0]]}
        assert "grant " in log_output
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # run it again, with no config changes
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        # seeds are always full-refreshed on this adapter, so we need to re-grant
        assert "revoke " not in log_output
        assert "grant " in log_output
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # change the grantee, assert it updates
        updated_yaml = self.interpolate_name_overrides(user2_schema_base_yml)
        write_file(updated_yaml, project.project_root, "seeds", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        expected = {select_privilege_name: ["user:" + test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # run it again, with --full-refresh, grants should be the same
        run_dbt(["seed", "--full-refresh"])
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # change config to 'grants: {}' -- should be completely ignored
        updated_yaml = self.interpolate_name_overrides(ignore_grants_yml)
        write_file(updated_yaml, project.project_root, "seeds", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        assert "revoke " not in log_output
        assert "grant " not in log_output
        expected_config = {}
        expected_actual = {select_privilege_name: [test_users[1]]}
        if self.seeds_support_partial_refresh():
            # ACTUAL grants will NOT match expected grants
            self.assert_expected_grants_match_actual(project, "my_seed", expected_actual)
        else:
            # there should be ZERO grants on the seed
            self.assert_expected_grants_match_actual(project, "my_seed", expected_config)

        # now run with ZERO grants -- all grants should be removed
        # whether explicitly (revoke) or implicitly (recreated without any grants added on)
        updated_yaml = self.interpolate_name_overrides(zero_grants_yml)
        write_file(updated_yaml, project.project_root, "seeds", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        if self.seeds_support_partial_refresh():
            assert "revoke " in log_output
        expected = {}
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # run it again -- dbt shouldn't try to grant or revoke anything
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        assert "revoke " not in log_output
        assert "grant " not in log_output
        self.assert_expected_grants_match_actual(project, "my_seed", expected)
