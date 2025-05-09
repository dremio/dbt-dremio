import pytest
from dbt.tests.util import run_dbt

class TestExactSearchFlagEnabled:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_model.sql": """
            {{ config(materialized='view') }}
            select table_name
            from information_schema."tables"
            where {{ "table_schema = 'my_schema'" if var('dremio:exact_search_enabled', false) 
            else "ilike(table_schema, 'my_schema')" }}
            """
        }

    def test_exact_search_enabled(self, project):
        results = run_dbt(["run", "--vars", '{"dremio:exact_search_enabled": true}'])
        assert len(results) > 0

        compiled_sql = results[0].node.compiled_code
        # Assert direct comparison is used
        assert "table_schema = 'my_schema'" in compiled_sql
        assert "ilike" not in compiled_sql

    def test_exact_search_disabled(self, project):
        results = run_dbt(["run", "--vars", '{"dremio:exact_search_enabled": false}'])
        assert len(results) > 0

        compiled_sql = results[0].node.compiled_code
        # Assert ilike is used
        assert "ilike(table_schema, 'my_schema')" in compiled_sql
        assert "table_schema = " not in compiled_sql