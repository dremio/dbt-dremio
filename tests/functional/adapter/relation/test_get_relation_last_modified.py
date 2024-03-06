import pytest
from dbt.tests.util import run_dbt
from tests.utils.util import BUCKET


freshness_via_metadata_schema_yml = """version: 2
sources:
  - name: test_source
    freshness:
      warn_after: {count: 10, period: hour}
      error_after: {count: 1, period: day}
    database: "dbt_test_source"  
    schema: "{{ target.schema }}"
    tables:
      - name: test_source
"""


seed = """
id,name
1,Martin
2,Jeter
3,Ruth
4,Gehrig
5,DiMaggio
6,Torre
7,Mantle
8,Berra
9,Maris
""".strip()


class TestGetLastRelationModified:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
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
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"test_source.csv": seed}

    def test_get_last_relation_modified(self, project):

        # run command
        result = run_dbt(["seed"])

        results = run_dbt(["source", "freshness"])
        assert len(results) == 1
        result = results[0]
        assert result.status == "pass"
