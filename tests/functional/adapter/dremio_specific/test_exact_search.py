import pytest
from dbt.tests.util import run_dbt

class TestExactSearch:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_model.sql": """
            {{ config(materialized='view') }}
            select table_name
            from information_schema."tables"
            where table_schema = 'my_schema'
            """
        }

    def test_exact_search(self, project):
        results = run_dbt(["run"])
        assert len(results) > 0

        compiled_sql = results[0].node.compiled_code
        assert "table_schema = 'my_schema'" in compiled_sql