import pytest
from dbt.tests.util import run_dbt

class TestExactSearchEnabled:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_model.sql": """
            {%- if var('dremio:exact_search_enabled', default=false) -%}
                SELECT 'EXACT_SEARCH_ENABLED' AS flag
            {%- else -%}
                SELECT 'EXACT_SEARCH_DISABLED' AS flag
            {%- endif -%}
            """
        }

    def test_exact_search_enabled(self, project):
        results = run_dbt(["run", "--vars", '{"dremio:exact_search_enabled": true}'])
        assert len(results) > 0

        compiled_sql = results[0].node.compiled_code
        assert "EXACT_SEARCH_ENABLED" in compiled_sql


    def test_exact_search_disabled(self, project):
        results = run_dbt(["run", "--vars", '{"dremio:exact_search_enabled": false}'])
        assert len(results) > 0

        compiled_sql = results[0].node.compiled_code
        assert "EXACT_SEARCH_DISABLED" in compiled_sql