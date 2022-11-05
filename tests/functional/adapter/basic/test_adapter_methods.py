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
