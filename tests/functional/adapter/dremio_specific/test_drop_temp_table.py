# Copyright (C) 2022 Dremio Corporation

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
from dbt.tests.util import run_dbt, get_connection, rm_file, write_file
from tests.utils.util import relation_from_name
from tests.fixtures.profiles import unique_schema, dbt_profile_data

initial_incremental_model = """
{{config(materialized='incremental')}}
select * from Samples."samples.dremio.com"."NYC-taxi-trips-iceberg" limit 10
{% if is_incremental() %}
{% endif %}
"""

subsequent_incremental_model = """
{{config(materialized='incremental')}}
select * from Samples."samples.dremio.com"."NYC-taxi-trips-iceberg" limit 20
{% if is_incremental() %}
{% endif %}
"""


class TestDropTempTableDremio:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_test.sql": initial_incremental_model,
        }

    def test_drop_temp_table(self, project):
        run_dbt(["run", "--select", "incremental_test"])

        rm_file(project.project_root, "models", "incremental_test.sql")

        write_file(
            subsequent_incremental_model,
            project.project_root,
            "models",
            "incremental_test.sql",
        )

        # The first run of an incremental model only builds the model
        run_dbt(["run"])

        temp_relation = relation_from_name(project.adapter, "incremental_test__dbt_tmp")

        with get_connection(project.adapter):
            columns = project.adapter.get_columns_in_relation(temp_relation)

        assert len(columns) == 0
