import pytest
from dbt.tests.util import run_dbt, get_connection

from tests.utils.util import relation_from_name


class TestExactSearchEnabled:

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "create_table.sql": """
                {{ config(
                    materialized='table',
                    schema='schema') }}
                select 1 as ilike
            """
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_table(self, project):
        run_dbt(["run", "--full-refresh",])

    def test_exact_match_succeeds(self, project):
        schema = project.test_schema
        run_dbt(["run", "--vars", '{"dremio:exact_search_enabled": true}'])

        adapter = project.adapter
        with get_connection(project.adapter):
            exists = adapter.check_schema_exists(project.database, schema)
            assert exists

    def test_exact_match_fails_on_case_mismatch(self, project):
        schema = project.test_schema
        run_dbt(["run", "--vars", '{"dremio:exact_search_enabled": true}'])

        adapter = project.adapter
        with get_connection(project.adapter):
            exists = adapter.check_schema_exists(project.database, schema.upper())
            assert not exists

    def test_ilike_match_succeeds(self, project):
        schema = project.test_schema
        run_dbt(["run", "--vars", '{"dremio:exact_search_enabled": false}'])

        adapter = project.adapter
        with get_connection(project.adapter):
            exists = adapter.check_schema_exists(project.database, schema.upper())
            assert exists

    def test_exact_match_get_columns(self, project):
        schema = "schema.create_table"
        run_dbt(["run", "--vars", '{"dremio:exact_search_enabled": true}'])

        table_relation = relation_from_name(
            project.adapter, schema)
        with get_connection(project.adapter):
            columns = project.adapter.get_columns_in_relation(table_relation)

        assert len(columns) == 1

    def test_ilike_get_columns(self, project):
        schema = "schema.create_table"
        run_dbt(["run", "--vars", '{"dremio:exact_search_enabled": false}'])

        table_relation = relation_from_name(
            project.adapter, schema)
        with get_connection(project.adapter):
            columns = project.adapter.get_columns_in_relation(table_relation)

        assert len(columns) == 1

    def test_exact_match_succeeds_list_relations_without_caching(self, project):
        run_dbt(["run", "--vars", '{"dremio:exact_search_enabled": true}'])

        adapter = project.adapter

        table_relation = relation_from_name(
            project.adapter, "schema.create_table")

        with get_connection(adapter):
            columns = adapter.list_relations_without_caching(table_relation)

        assert len(columns) == 1

    def test_exact_match_fails_list_relations_without_caching(self, project):
        run_dbt(["run", "--vars", '{"dremio:exact_search_enabled": true}'])

        adapter = project.adapter
        table_relation = relation_from_name(
            project.adapter, "SCHEMA.create_table")

        with get_connection(adapter):
            columns = adapter.list_relations_without_caching(table_relation)

        assert len(columns) == 0

    def test_ilike_list_relations_without_caching(self, project):
        run_dbt(["run", "--vars", '{"dremio:exact_search_enabled": false}'])

        adapter = project.adapter
        table_relation = relation_from_name(
            project.adapter, "schema.create_table")

        with get_connection(adapter):
            columns = adapter.list_relations_without_caching(table_relation)

        assert len(columns) == 1

    def test_ilike_case_sensitive_list_relations_without_caching(self, project):
        run_dbt(["run", "--vars", '{"dremio:exact_search_enabled": false}'])

        adapter = project.adapter
        table_relation = relation_from_name(
            project.adapter, "SCHEMA.create_table")

        with get_connection(adapter):
            columns = adapter.list_relations_without_caching(table_relation)

        assert len(columns) == 1