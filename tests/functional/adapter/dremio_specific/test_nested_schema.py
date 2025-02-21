import pytest
from dbt.tests.util import run_dbt, get_connection
from tests.utils.util import relation_from_name


custom_schema_table_no_schema_model = """
{{ config(
    materialized='table'
) }}
select 1 as id
"""

custom_schema_table_model = """
{{ config(
    materialized='table',
    schema='schema'
) }}
select 1 as id
"""

custom_schema_table_nested_model = """
{{ config(
    materialized='table',
    schema='nested.schema'
) }}
select 1 as id
"""

custom_schema_view_no_schema_model = """
{{ config(
    materialized='view'
) }}
select 1 as id
"""

custom_schema_view_model = """
{{ config(
    materialized='view',
    schema='schema'
) }}
select 1 as id
"""

custom_schema_view_nested_model = """
{{ config(
    materialized='view',
    schema='nested.schema'
) }}
select 1 as id
"""


class TestGetCustomSchema:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "custom_schema_table_no_schema.sql": custom_schema_table_no_schema_model,
            "custom_schema_table.sql": custom_schema_table_model,
            "custom_schema_table_nested.sql": custom_schema_table_nested_model,
            "custom_schema_view_no_schema.sql": custom_schema_view_no_schema_model,
            "custom_schema_view.sql": custom_schema_view_model,
            "custom_schema_view_nested.sql": custom_schema_view_nested_model,
        }

    def test_custom_schema_table_no_schema(self, project):
        run_dbt(["run", "--select", "custom_schema_table_no_schema"])
        table_relation = relation_from_name(
            project.adapter, "custom_schema_table_no_schema")
        with get_connection(project.adapter):
            columns = project.adapter.get_columns_in_relation(table_relation)
        assert len(columns) == 1
        assert columns[0].name == "id"

    def test_custom_schema_table(self, project):
        run_dbt(["run", "--select", "custom_schema_table"])
        table_relation = relation_from_name(
            project.adapter, "schema.custom_schema_table")
        with get_connection(project.adapter):
            columns = project.adapter.get_columns_in_relation(table_relation)
        assert len(columns) == 1
        assert columns[0].name == "id"

    def test_custom_schema_table_nested(self, project):
        run_dbt(["run", "--select", "custom_schema_table_nested"])
        table_relation = relation_from_name(
            project.adapter, "nested.schema.custom_schema_table_nested")
        with get_connection(project.adapter):
            columns = project.adapter.get_columns_in_relation(table_relation)
        assert len(columns) == 1
        assert columns[0].name == "id"

    def test_custom_schema_view_no_schema(self, project):
        run_dbt(["run", "--select", "custom_schema_view_no_schema"])
        view_relation = relation_from_name(
            project.adapter, "custom_schema_view_no_schema")
        with get_connection(project.adapter):
            columns = project.adapter.get_columns_in_relation(view_relation)
        assert len(columns) == 1
        assert columns[0].name == "id"

    def test_custom_schema_view(self, project):
        run_dbt(["run", "--select", "custom_schema_view"])
        view_relation = relation_from_name(
            project.adapter, "schema.custom_schema_view")
        with get_connection(project.adapter):
            columns = project.adapter.get_columns_in_relation(view_relation)
        assert len(columns) == 1
        assert columns[0].name == "id"

    def test_custom_schema_view_nested(self, project):
        run_dbt(["run", "--select", "custom_schema_view_nested"])
        view_relation = relation_from_name(
            project.adapter, "nested.schema.custom_schema_view_nested")
        with get_connection(project.adapter):
            columns = project.adapter.get_columns_in_relation(view_relation)
        assert len(columns) == 1
        assert columns[0].name == "id"
