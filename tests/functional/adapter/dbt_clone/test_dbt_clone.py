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
from tests.fixtures.profiles import unique_schema

from copy import deepcopy
from tests.utils.util import BUCKET, SOURCE
from dbt.tests.adapter.dbt_clone.test_dbt_clone import BaseCloneNotPossible
from dbt.tests.adapter.dbt_clone import fixtures
from dbt.tests.util import run_dbt


# Need to change the target_database to be the object_storage_source
snapshot_sql = """
{% snapshot my_cool_snapshot %}

    {{
        config(
            target_database="dbt_test_source",
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['id'],
        )
    }}
    select * from {{ ref('view_model') }}

{% endsnapshot %}
"""

# Append _seeds to schemas of both targets
get_schema_name_sql = """
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is not none -%}
        {{ return(default_schema ~ '_' ~ custom_schema_name|trim) }}
    {%- elif node.resource_type == 'seed' -%}
        {{ return(default_schema ~ '_' ~ 'seeds') }}
    {%- else -%}
        {{ return(default_schema) }}
    {%- endif -%}
{%- endmacro %}
"""


class TestCloneNotPossibleDremio(BaseCloneNotPossible):

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot.sql": snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macros.sql": fixtures.macros_sql,
            "my_can_clone_tables.sql": fixtures.custom_can_clone_tables_false_macros_sql,
            "infinite_macros.sql": fixtures.infinite_macros_sql,
            "get_schema_name.sql": get_schema_name_sql,
        }

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema, other_schema):
        outputs = {
            "default": dbt_profile_target,
            "otherschema": deepcopy(dbt_profile_target),
        }
        outputs["default"]["schema"] = unique_schema
        outputs["otherschema"]["schema"] = other_schema
        outputs["otherschema"]["root_path"] = unique_schema + "_seeds"
        return {"test": {"outputs": outputs, "target": "default"}}

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
        target["root_path"] = unique_schema
        profile["test"]["outputs"]["default"] = target

        if profiles_config_update:
            profile.update(profiles_config_update)
        return profile

    def test_can_clone_false(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)
        self.run_and_save_state(project.project_root, with_snapshot=True)

        clone_args = [
            "clone",
            "--state",
            "state",
            "--target",
            "otherschema",
        ]

        results = run_dbt(clone_args, False)
