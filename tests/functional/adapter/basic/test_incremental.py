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
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
)
from dbt.tests.adapter.incremental.test_incremental_merge_exclude_columns import (
    BaseMergeExcludeColumns,
)
from tests.fixtures.profiles import unique_schema, dbt_profile_data
from tests.utils.util import BUCKET, SOURCE
from dbt.tests.util import run_dbt
from collections import namedtuple


models__merge_exclude_columns_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy='merge',
    merge_exclude_columns='msg'
) }}

{% if not is_incremental() %}

-- data for first invocation of model

select 1 as id, 'hello' as msg, 'blue' as color
union all
select 2 as id, 'goodbye' as msg, 'red' as color

{% else %}

-- data for subsequent incremental update

select 1 as id, 'hey' as msg, 'blue' as color
union all
select 2 as id, 'yo' as msg, 'green' as color
union all
select 3 as id, 'anyway' as msg, 'purple' as color

{% endif %}
"""

ResultHolder = namedtuple(
    "ResultHolder",
    [
        "seed_count",
        "model_count",
        "seed_rows",
        "inc_test_model_count",
        "relation",
    ],
)


class TestIncrementalDremio(BaseIncremental):
    pass


class TestBaseIncrementalNotSchemaChange(BaseIncrementalNotSchemaChange):
    pass


class TestBaseMergeExcludeColumnsDremio(BaseMergeExcludeColumns):
    def get_test_fields(self, project, seed, incremental_model, update_sql_file):
        seed_count = len(run_dbt(["seed", "--select", seed, "--full-refresh"]))

        model_count = len(
            run_dbt(["run", "--select", incremental_model, "--full-refresh"])
        )

        relation = incremental_model
        # update seed in anticipation of incremental model update
        row_count_query = "select * from {}.{}".format(
            f"{SOURCE}.{BUCKET}.{project.test_schema}", seed
        )

        seed_rows = len(project.run_sql(row_count_query, fetch="all"))

        # propagate seed state to incremental model according to unique keys
        inc_test_model_count = self.update_incremental_model(
            incremental_model=incremental_model
        )

        return ResultHolder(
            seed_count, model_count, seed_rows, inc_test_model_count, relation
        )
