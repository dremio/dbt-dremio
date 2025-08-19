import os
import pytest
from tests.fixtures.profiles import unique_schema, dbt_profile_data

from dbt.tests.util import run_dbt, write_file
from tests.utils.util import (
    check_relation_types,
    relation_from_name,
    get_connection,
    BUCKET
)

DREMIO_EDITION = os.getenv("DREMIO_EDITION")
DREMIO_ENTERPRISE_CATALOG = os.getenv("DREMIO_ENTERPRISE_CATALOG")

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

class TestTwinStrategyNotAppliedDremio:
    # Override unique_schema to be the schema defined in Jenkins tests, i.e., tests_functional_adapter_dremio_specific
    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file_path = request.module.__file__
        relative_path = os.path.relpath(test_file_path, os.getcwd())
        # Get directory path and remove filename
        dir_path = os.path.dirname(relative_path)
        # Replace '/' with '_' to create schema name
        test_file = dir_path.replace('/', '_')
        unique_schema = test_file
        return unique_schema

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_allow_yml,
            "view_model.sql": view_model_sql,
            "table_model.sql": table_model_sql,
        }

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
        # For enterprise catalog: object_storage_source == dremio_space AND object_storage_path == dremio_space_folder
        # This maps to: target.datalake == target.database AND target.root_path == target.schema
        enterprise_catalog_name = DREMIO_ENTERPRISE_CATALOG
        target["schema"] = unique_schema
        target["root_path"] = unique_schema # Make object_storage_path == dremio_space_folder
        target["datalake"] = enterprise_catalog_name  # Set object_storage_source to enterprise catalog
        target["database"] = enterprise_catalog_name  # Set dremio_space to same enterprise catalog

        profile["test"]["outputs"]["default"] = target
        if profiles_config_update:
            profile.update(profiles_config_update)
        return profile

    @pytest.mark.skipif(DREMIO_EDITION == "community" or not os.getenv("DREMIO_ENTERPRISE_CATALOG"), reason="Enterprise catalog is only supported in Dremio EE/DC editions.")
    def test_twin_strategy_not_applied_with_enterprise_catalog(self, project, caplog):
        # Run with twin_strategy configured but enterprise catalog enabled
        # Should show warning and not apply twin strategy
        run_dbt(["run"])

        # Check that the warning message appears in the logs
        assert "WARNING: Twin strategy not applied - using enterprise catalog" in caplog
