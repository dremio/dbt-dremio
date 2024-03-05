import os
import pytest

from dbt.cli.main import dbtRunner
from dbt.tests.util import run_dbt
from tests.utils.util import BUCKET


freshness_via_metadata_schema_yml = """version: 2
sources:
  - name: test_source
    freshness:
      warn_after: {count: 10, period: hour}
      error_after: {count: 1, period: day}
    database: "dbt_test_source"  
    schema: "dbtdremios3"
    tables:
      - name: test_table
"""

table = """
{{config(materialized = "table")}}
select 1 as id
"""


class TestGetLastRelationModified:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+twin_strategy": "prevent",
            },
        }

    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        # We only want the last part of the name
        test_file = test_file.split(".")[-1]
        unique_schema = f"{BUCKET}.{prefix}_{test_file}"
        return unique_schema

    @pytest.fixture(scope="class")
    def dbt_profile_data(
        self, unique_schema, dbt_profile_target, profiles_config_update
    ):
        profile = {
            "config": {"send_anonymous_usage_stats": False},
            "test": {
                "outputs": {
                    "default": {},
                },
                "target": "default",
            },
        }
        target = dbt_profile_target
        target["schema"] = unique_schema
        target["root_path"] = unique_schema
        profile["test"]["outputs"]["default"] = target

        if profiles_config_update:
            profile.update(profiles_config_update)
        return profile

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": freshness_via_metadata_schema_yml,
            "test_table.sql": table,
        }

    def test_get_last_relation_modified(self, project):

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 1

        warning_or_error = False

        def probe(e):
            nonlocal warning_or_error
            if e.info.level in ["warning", "error"]:
                warning_or_error = True

        runner = dbtRunner(callbacks=[probe])
        runner.invoke(["source", "freshness"])

        # The 'source freshness' command should succeed without warnings or errors.
        assert not warning_or_error
