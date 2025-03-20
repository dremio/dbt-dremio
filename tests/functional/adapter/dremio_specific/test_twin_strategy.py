import pytest
from tests.fixtures.profiles import unique_schema, dbt_profile_data

from dbt.tests.util import run_dbt, write_file
from tests.utils.util import (
    check_relation_types,
    relation_from_name,
    get_connection
)

schema_prevent_yml = """
version: 2
models:
  - name: view_model 
    config:
      twin_strategy: prevent
  - name: table_model
    config:
      twin_strategy: prevent
"""

schema_allow_yml = """
version: 2
models:
  - name: view_model 
    config:
      twin_strategy: allow
  - name: table_model
    config:
      twin_strategy: allow
"""

schema_clone_yml = """
version: 2
models:
  - name: view_model 
    config:
      twin_strategy: clone
  - name: table_model
    config:
      twin_strategy: clone
"""

view_model_sql = """
{{ config(
    materialized='view'
) }}
    select 1 as view_column
"""

table_model_sql = """
{{ config(
    materialized='table'
) }}
    select 1 as table_column
"""
view_model_conflict_sql = """
{{ config(
    materialized='view'
) }}
    select 1 as view_column_conflict
"""

table_model_conflict_sql = """
{{ config(
    materialized='table'
) }}
    select 1 as table_column_conflict
"""

class TestTwinStrategyPreventDremio:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_prevent_yml,
            "view_model.sql": view_model_sql,
            "table_model.sql": table_model_sql,
        }

    def test_overwrite_view(self, project):
        run_dbt(["run"])
        expected = {
            "view_model": "view",
        }
        check_relation_types(project.adapter, expected)

        # Create a table model that conflicts with the view model 
        write_file(table_model_conflict_sql, project.project_root, "models", "view_model.sql")
        run_dbt(["run"])

        # Ensure the view is not overwritten
        relation_view = relation_from_name(project.adapter, "view_model", materialization="view")
        relation_table = relation_from_name(project.adapter, "view_model", materialization="table") 
        with get_connection(project.adapter):
            columns_view = project.adapter.get_columns_in_relation(relation_view)
            columns_table = project.adapter.get_columns_in_relation(relation_table)

        assert len(columns_view) == 0 
        assert len(columns_table) == 1
        assert columns_table[0].name == "table_column_conflict"


    def test_overwrite_table(self, project):
        run_dbt(["run"])
        expected = {
            "table_model": "table",
        }
        check_relation_types(project.adapter, expected)

        # Create a view model that conflicts with the table model 
        write_file(view_model_conflict_sql, project.project_root, "models", "table_model.sql")
        run_dbt(["run"])

        # Ensure the table is not overwritten
        relation_view = relation_from_name(project.adapter, "table_model", materialization="view")
        relation_table = relation_from_name(project.adapter, "table_model", materialization="table") 
        with get_connection(project.adapter):
            columns_view = project.adapter.get_columns_in_relation(relation_view)
            columns_table = project.adapter.get_columns_in_relation(relation_table)

        assert len(columns_view) == 1 
        assert columns_view[0].name == "view_column_conflict"
        assert len(columns_table) == 0

class TestTwinStrategyAllowDremio:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_allow_yml,
            "view_model.sql": view_model_sql,
            "table_model.sql": table_model_sql,
        }

    def test_overwrite_view(self, project):
        run_dbt(["run"])
        expected = {
            "view_model": "view",
        }
        check_relation_types(project.adapter, expected)

        # Create a table model that conflicts with the view model 
        write_file(table_model_conflict_sql, project.project_root, "models", "view_model.sql")
        run_dbt(["run"])

        # Ensure both the view and the table exist independently
        relation_view = relation_from_name(project.adapter, "view_model", materialization="view")
        relation_table = relation_from_name(project.adapter, "view_model", materialization="table") 
        with get_connection(project.adapter):
            columns_view = project.adapter.get_columns_in_relation(relation_view)
            columns_table = project.adapter.get_columns_in_relation(relation_table)

        assert len(columns_view) == 1
        assert columns_view[0].name == "view_column"
        assert len(columns_table) == 1 
        assert columns_table[0].name == "table_column_conflict"

    def test_overwrite_table(self, project):
        run_dbt(["run"])
        expected = {
            "table_model": "table",
        }
        check_relation_types(project.adapter, expected)

        # Create a view model that conflicts with the table model 
        write_file(view_model_conflict_sql, project.project_root, "models", "table_model.sql")
        run_dbt(["run"])

        # Ensure both the view and the table exist independently 
        relation_view = relation_from_name(project.adapter, "table_model", materialization="view")
        relation_table = relation_from_name(project.adapter, "table_model", materialization="table") 
        with get_connection(project.adapter):
            columns_view = project.adapter.get_columns_in_relation(relation_view)
            columns_table = project.adapter.get_columns_in_relation(relation_table)

        assert len(columns_view) == 1 
        assert columns_view[0].name == "view_column_conflict"
        assert len(columns_table) == 1
        assert columns_table[0].name == "table_column"

class TestTwinStrategyCloneDremio:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_clone_yml,
            "view_model.sql": view_model_sql,
            "table_model.sql": table_model_sql,
        }

    def test_overwrite_view(self, project):
        run_dbt(["run"])
        expected = {
            "view_model": "view",
        }
        check_relation_types(project.adapter, expected)

        # Create a table model that conflicts with the view model 
        write_file(table_model_conflict_sql, project.project_root, "models", "view_model.sql")
        run_dbt(["run"])

        # Ensure the view is overwritten
        relation_view = relation_from_name(project.adapter, "view_model", materialization="view")
        relation_table = relation_from_name(project.adapter, "view_model", materialization="table") 
        with get_connection(project.adapter):
            columns_view = project.adapter.get_columns_in_relation(relation_view)
            columns_table = project.adapter.get_columns_in_relation(relation_table)

        assert len(columns_view) == 1
        assert columns_view[0].name == "table_column_conflict"
        assert len(columns_table) == 1 
        assert columns_table[0].name == "table_column_conflict"

    def test_overwrite_table(self, project):
        run_dbt(["run"])
        expected = {
            "table_model": "table",
        }
        check_relation_types(project.adapter, expected)

        # Create a view model that conflicts with the table model 
        write_file(view_model_conflict_sql, project.project_root, "models", "table_model.sql")
        run_dbt(["run"])

        # Ensure the view is set to select from the table
        relation_view = relation_from_name(project.adapter, "table_model", materialization="view")
        relation_table = relation_from_name(project.adapter, "table_model", materialization="table") 
        with get_connection(project.adapter):
            columns_view = project.adapter.get_columns_in_relation(relation_view)
            columns_table = project.adapter.get_columns_in_relation(relation_table)

        assert len(columns_view) == 1
        assert columns_view[0].name == "table_column" 
        assert len(columns_table) == 1
        assert columns_table[0].name == "table_column"
