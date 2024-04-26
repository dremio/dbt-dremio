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
from dbt.tests.adapter.utils.test_date_spine import BaseDateSpine
from dbt.tests.adapter.utils.fixture_date_spine import models__test_date_spine_yml
from tests.utils.util import BUCKET
from tests.fixtures.profiles import unique_schema, dbt_profile_data

models__test_date_spine_sql = """
with generated_dates as (
    {% if target.type == 'postgres' %}
        {{ date_spine("day", "'2023-09-01'::date", "'2023-09-10'::date") }}

    {% elif target.type == 'bigquery' or target.type == 'redshift' %}
        select cast(date_day as date) as date_day
        from ({{ date_spine("day", "'2023-09-01'", "'2023-09-10'") }})

    {% else %}
        {{ date_spine("day", "'2023-09-01'", "'2023-09-10'") }}
    {% endif %}
), expected_dates as (

        select '2023-09-01' as expected
        union all
        select '2023-09-02' as expected
        union all
        select '2023-09-03' as expected
        union all
        select '2023-09-04' as expected
        union all
        select '2023-09-05' as expected
        union all
        select '2023-09-06' as expected
        union all
        select '2023-09-07' as expected
        union all
        select '2023-09-08' as expected
        union all
        select '2023-09-09' as expected

), joined as (
    select
        generated_dates.date_day,
        expected_dates.expected
    from generated_dates
    left join expected_dates on CAST(generated_dates.date_day as TIMESTAMP) = CAST(expected_dates.expected as TIMESTAMP)
)

SELECT * from joined
"""


class TestDateSpine(BaseDateSpine):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_spine.yml": models__test_date_spine_yml,
            "test_date_spine.sql": self.interpolate_macro_namespace(
                models__test_date_spine_sql, "date_spine"
            ),
        }
