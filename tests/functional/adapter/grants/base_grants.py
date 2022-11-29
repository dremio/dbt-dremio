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

from dbt.tests.adapter.grants.base_grants import BaseGrants
from tests.utils.util import BUCKET


class BaseGrantsDremio(BaseGrants):
    # This is overridden to change insert privilege to alter
    def privilege_grantee_name_overrides(self):
        return {
            "select": "select",
            "insert": "alter",
            "fake_privilege": "fake_privilege",
            "invalid_user": "invalid_user",
        }

    # This is overridden to make sure tables aren't cloned as views
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+twin_strategy": "prevent",
            },
            "seeds": {"+twin_strategy": "prevent"},
            "vars": {"dremio:reflections": "false"},
        }

    @pytest.fixture(scope="class")
    def dbt_profile_data(self, unique_schema, dbt_profile_target, profiles_config_update):
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
        target["schema"] = unique_schema
        target["root_path"] = f"{BUCKET}.{unique_schema}"
        profile["test"]["outputs"]["default"] = target

        if profiles_config_update:
            profile.update(profiles_config_update)
        return profile
