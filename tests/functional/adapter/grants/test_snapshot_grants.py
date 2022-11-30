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
from dbt.tests.adapter.grants.test_snapshot_grants import (
    BaseSnapshotGrants,
    snapshot_schema_yml,
)
from tests.functional.adapter.grants.base_grants import BaseGrantsDremio
from tests.utils.util import (
    relation_from_name,
    BUCKET,
    SOURCE,
)
from dbt.tests.util import get_connection

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
            "config": {"send_anonymous_usage_stats": False},
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
