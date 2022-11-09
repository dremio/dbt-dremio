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
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_adapter_methods import models__upstream_sql
from tests.fixtures.profiles import unique_schema, dbt_profile_data

models__my_model_sql = """

{% set upstream = ref('upstream_view') %}

{% if execute %}
    {# don't ever do any of this #}
    {%- do adapter.drop_schema(upstream) -%}
    {% set existing = adapter.get_relation(upstream.database, upstream.schema, upstream.identifier) %}
    {% if existing is not defined %}
        {% do exceptions.raise_compiler_error('expected ' ~ ' to not exist, but it did') %}
    {% endif %}

    {%- do adapter.create_schema(upstream) -%}

    {% set sql = create_view_as(upstream, 'select 2 as id') %}
    {% do run_query(sql) %}
{% endif %}


select * from {{ upstream }}

"""

models__expected_sql = """
-- make sure this runs after 'model'
-- {{ ref('model_view') }}
select 2 as id

"""


class TestBaseAdapterMethodDremio(BaseAdapterMethod):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+twin_strategy": "clone",
            },
            "name": "adapter_methods",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream_view.sql": models__upstream_sql,
            "expected_view.sql": models__expected_sql,
            "model_view.sql": models__my_model_sql,
        }

    @pytest.fixture(scope="class")
    def equal_tables(self):
        return ["model_view", "expected_view"]
