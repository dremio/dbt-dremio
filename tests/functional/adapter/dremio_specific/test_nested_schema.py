import pytest
from dbt.tests.util import run_dbt, get_connection
from tests.fixtures.profiles import unique_schema, dbt_profile_data


# Table tests - should use root_path config
custom_schema_table_no_schema_model = """
{{ config(
    materialized='table'
) }}
select 1 as id
"""

custom_schema_table_with_root_path_model = """
{{ config(
    materialized='table',
    root_path='schema'
) }}
select 1 as id
"""

custom_schema_table_with_schema_config_model = """
{{ config(
    materialized='table',
    schema='useless_config'
) }}
select 1 as id
"""

custom_schema_table_nested_with_root_path_model = """
{{ config(
    materialized='table',
    root_path='nested.schema'
) }}
select 1 as id
"""

# View tests - should use schema config
custom_schema_view_no_schema_model = """
{{ config(
    materialized='view'
) }}
select 1 as id
"""

custom_schema_view_with_schema_config_model = """
{{ config(
    materialized='view',
    schema='schema'
) }}
select 1 as id
"""

custom_schema_view_with_root_path_config_model = """
{{ config(
    materialized='view',
    root_path='useless_config'
) }}
select 1 as id
"""

custom_schema_view_nested_with_schema_config_model = """
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
            # Table models
            "custom_schema_table_no_schema.sql": custom_schema_table_no_schema_model,
            "custom_schema_table_with_root_path.sql": custom_schema_table_with_root_path_model,
            "custom_schema_table_with_schema_config.sql": custom_schema_table_with_schema_config_model,
            "custom_schema_table_nested_with_root_path.sql": custom_schema_table_nested_with_root_path_model,
            # View models
            "custom_schema_view_no_schema.sql": custom_schema_view_no_schema_model,
            "custom_schema_view_with_schema_config.sql": custom_schema_view_with_schema_config_model,
            "custom_schema_view_with_root_path_config.sql": custom_schema_view_with_root_path_config_model,
            "custom_schema_view_nested_with_schema_config.sql": custom_schema_view_nested_with_schema_config_model,
        }

    # ===== TABLE TESTS =====

    def test_custom_schema_table_no_schema(self, project):
        """Test table without custom schema - should use default root_path from profile"""
        run_dbt(["run", "--select", "custom_schema_table_no_schema"])

        # Expected path components
        credentials = project.adapter.config.credentials
        expected_database = credentials.datalake
        expected_schema = credentials.root_path  # e.g., "dbtdremios3.test17636798006768308215_test_nested_schema"
        expected_identifier = "custom_schema_table_no_schema"

        # Get the actual relation from Dremio
        with get_connection(project.adapter):
            actual_relation = project.adapter.get_relation(
                database=expected_database,
                schema=expected_schema,
                identifier=expected_identifier
            )

            # Verify the relation was created with the expected path
            assert actual_relation is not None, f"Table should have been created at {expected_database}.{expected_schema}.{expected_identifier}"
            assert actual_relation.type == "table"

            columns = project.adapter.get_columns_in_relation(actual_relation)
            assert len(columns) == 1
            assert columns[0].name == "id"

    def test_custom_schema_table_with_root_path(self, project):
        """Test table with root_path config - should be appended to default root_path"""
        run_dbt(["run", "--select", "custom_schema_table_with_root_path"])

        # Expected path components
        credentials = project.adapter.config.credentials
        expected_database = credentials.datalake
        expected_schema = f"{credentials.root_path}.schema"
        expected_identifier = "custom_schema_table_with_root_path"

        # Get the actual relation from Dremio
        with get_connection(project.adapter):
            actual_relation = project.adapter.get_relation(
                database=expected_database,
                schema=expected_schema,
                identifier=expected_identifier
            )

            # Verify the relation was created with the expected path
            assert actual_relation is not None, f"Table should have been created at {expected_database}.{expected_schema}.{expected_identifier}"
            assert actual_relation.type == "table"

            # Verify we can query it
            columns = project.adapter.get_columns_in_relation(actual_relation)
            assert len(columns) == 1
            assert columns[0].name == "id"

    def test_custom_schema_table_with_schema_config(self, project):
        """Test table with schema config - should be IGNORED for tables (root_path is used instead)"""
        run_dbt(["run", "--select", "custom_schema_table_with_schema_config"])

        # Expected path components
        credentials = project.adapter.config.credentials
        expected_database = credentials.datalake
        expected_schema = credentials.root_path # schema config is ignored for tables
        expected_identifier = "custom_schema_table_with_schema_config"

        # Get the actual relation from Dremio
        with get_connection(project.adapter):
            actual_relation = project.adapter.get_relation(
                database=expected_database,
                schema=expected_schema,
                identifier=expected_identifier
            )

            # Verify the relation was created with the expected path
            assert actual_relation is not None, f"Table should have been created at {expected_database}.{expected_schema}.{expected_identifier}"
            assert actual_relation.type == "table"

            # Verify we can query it
            columns = project.adapter.get_columns_in_relation(actual_relation)
            assert len(columns) == 1
            assert columns[0].name == "id"

    def test_custom_schema_table_nested_with_root_path(self, project):
        """Test table with nested root_path config - should be appended to default root_path"""
        run_dbt(["run", "--select", "custom_schema_table_nested_with_root_path"])

        # Expected path components
        credentials = project.adapter.config.credentials
        expected_database = credentials.datalake
        expected_schema = f"{credentials.root_path}.nested.schema"
        expected_identifier = "custom_schema_table_nested_with_root_path"

        # Get the actual relation from Dremio
        with get_connection(project.adapter):
            actual_relation = project.adapter.get_relation(
                database=expected_database,
                schema=expected_schema,
                identifier=expected_identifier
            )

            # Verify the relation was created with the expected path
            assert actual_relation is not None, f"Table should have been created at {expected_database}.{expected_schema}.{expected_identifier}"
            assert actual_relation.type == "table"

            # Verify we can query it
            columns = project.adapter.get_columns_in_relation(actual_relation)
            assert len(columns) == 1
            assert columns[0].name == "id"

    # ===== VIEW TESTS =====

    def test_custom_schema_view_no_schema(self, project):
        """Test view without custom schema - should use default schema from profile"""
        run_dbt(["run", "--select", "custom_schema_view_no_schema"])

        # Expected path components
        credentials = project.adapter.config.credentials
        expected_database = credentials.database
        expected_schema = credentials.schema  # e.g., "test17636798006768308215_test_nested_schema"
        expected_identifier = "custom_schema_view_no_schema"

        # Get the actual relation from Dremio
        with get_connection(project.adapter):
            actual_relation = project.adapter.get_relation(
                database=expected_database,
                schema=expected_schema,
                identifier=expected_identifier
            )

            # Verify the relation was created with the expected path
            assert actual_relation is not None, f"View should have been created at {expected_database}.{expected_schema}.{expected_identifier}"
            assert actual_relation.type == "view"

            # Verify we can query it
            columns = project.adapter.get_columns_in_relation(actual_relation)
            assert len(columns) == 1
            assert columns[0].name == "id"

    def test_custom_schema_view_with_schema_config(self, project):
        """Test view with schema config - should be appended to default schema"""
        run_dbt(["run", "--select", "custom_schema_view_with_schema_config"])

        # Expected path components
        credentials = project.adapter.config.credentials
        expected_database = credentials.database
        expected_schema = f"{credentials.schema}.schema"  # Appended!
        expected_identifier = "custom_schema_view_with_schema_config"

        # Get the actual relation from Dremio
        with get_connection(project.adapter):
            actual_relation = project.adapter.get_relation(
                database=expected_database,
                schema=expected_schema,
                identifier=expected_identifier
            )

            # Verify the relation was created with the expected path
            assert actual_relation is not None, f"View should have been created at {expected_database}.{expected_schema}.{expected_identifier}"
            assert actual_relation.type == "view"

            # Verify we can query it
            columns = project.adapter.get_columns_in_relation(actual_relation)
            assert len(columns) == 1
            assert columns[0].name == "id"

    def test_custom_schema_view_with_root_path_config(self, project):
        """Test view with root_path config - should be IGNORED for views (schema is used instead)"""
        run_dbt(["run", "--select", "custom_schema_view_with_root_path_config"])

        # Expected path components - root_path config should be ignored for views
        credentials = project.adapter.config.credentials
        expected_database = credentials.database
        expected_schema = credentials.schema # root_path config is ignored for views
        expected_identifier = "custom_schema_view_with_root_path_config"

        # Get the actual relation from Dremio
        with get_connection(project.adapter):
            actual_relation = project.adapter.get_relation(
                database=expected_database,
                schema=expected_schema,
                identifier=expected_identifier
            )

            # Verify the relation was created with the expected path (root_path config ignored)
            assert actual_relation is not None, f"View should have been created at {expected_database}.{expected_schema}.{expected_identifier} (root_path config should be ignored for views)"
            assert actual_relation.type == "view"

            # Verify we can query it
            columns = project.adapter.get_columns_in_relation(actual_relation)
            assert len(columns) == 1
            assert columns[0].name == "id"

    def test_custom_schema_view_nested_with_schema_config(self, project):
        """Test view with nested schema config - should be appended to default schema"""
        run_dbt(["run", "--select", "custom_schema_view_nested_with_schema_config"])

        # Expected path components
        credentials = project.adapter.config.credentials
        expected_database = credentials.database
        expected_schema = f"{credentials.schema}.nested.schema"  # Appended!
        expected_identifier = "custom_schema_view_nested_with_schema_config"

        # Get the actual relation from Dremio
        with get_connection(project.adapter):
            actual_relation = project.adapter.get_relation(
                database=expected_database,
                schema=expected_schema,
                identifier=expected_identifier
            )

            # Verify the relation was created with the expected path
            assert actual_relation is not None, f"View should have been created at {expected_database}.{expected_schema}.{expected_identifier}"
            assert actual_relation.type == "view"

            # Verify we can query it
            columns = project.adapter.get_columns_in_relation(actual_relation)
            assert len(columns) == 1
            assert columns[0].name == "id"
