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
from dbt.tests.adapter.grants.test_snapshot_grants import (
    BaseSnapshotGrants,
    snapshot_schema_yml,
    user2_snapshot_schema_yml,
)
from tests.functional.adapter.grants.base_grants import BaseGrantsDremio
from tests.utils.util import (
    relation_from_name,
    BUCKET,
    SOURCE,
)
from dbt.tests.util import (
    get_connection,
    get_manifest,
    run_dbt,
    run_dbt_and_capture,
    write_file,
)

# Override this model to use strategy timestamp
# we use timestamp for now, as 'check' is not supported
my_snapshot_sql = """
{% snapshot my_snapshot %}
    {{ config(
        updated_at='id', unique_key='id', strategy='timestamp',
        target_database=database, target_schema=schema
    ) }}
    select 1 as id, cast('blue' as {{ type_string() }}) as color
{% endsnapshot %}
""".strip()

DREMIO_EDITION = os.getenv("DREMIO_EDITION")

@pytest.mark.skipif(DREMIO_EDITION == "community", reason="Dremio only supports grants in EE/DC editions.")
class TestSnapshotGrantsDremio(BaseGrantsDremio, BaseSnapshotGrants):
    # Override this to use our modified snapshot model
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "my_snapshot.sql": my_snapshot_sql,
            "schema.yml": self.interpolate_name_overrides(snapshot_schema_yml),
        }

    # Need to set target_database=datalake, and target_schema=root_path
    # These are necessary and defined in the model config
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
        target["schema"] = f"{BUCKET}.{unique_schema}"
        target["root_path"] = f"{BUCKET}.{unique_schema}"
        target["database"] = SOURCE
        profile["test"]["outputs"]["default"] = target

        if profiles_config_update:
            profile.update(profiles_config_update)
        return profile

    # Override this to use our version of relation_from_name
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
    def test_snapshot_grants(self, project, get_test_users):
        test_users = get_test_users
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]

        # run the snapshot
        results = run_dbt(["snapshot"])
        assert len(results) == 1
        expected = {select_privilege_name: ["user:" + test_users[0]]}
        self.assert_expected_grants_match_actual(project, "my_snapshot", expected)

        # run it again, nothing should have changed
        (results, log_output) = run_dbt_and_capture(["--debug", "snapshot"])
        assert len(results) == 1
        assert "revoke " not in log_output
        assert "grant " not in log_output
        self.assert_expected_grants_match_actual(project, "my_snapshot", expected)

        # change the grantee, assert it updates
        updated_yaml = self.interpolate_name_overrides(user2_snapshot_schema_yml)
        write_file(updated_yaml, project.project_root, "snapshots", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "snapshot"])
        assert len(results) == 1
        expected = {select_privilege_name: ["user:" + test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_snapshot", expected)
