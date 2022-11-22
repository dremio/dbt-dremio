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

from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from tests.functional.adapter.grants.base_grants import BaseGrantsDremio
from tests.utils.util import relation_from_name
from dbt.tests.util import get_connection


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
