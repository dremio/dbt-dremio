import pytest
from dbt.tests.adapter.basic.test_incremental import BaseIncremental


class TestIncrementalDremio(BaseIncremental):
    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        # We only want the last part of the name
        test_file = test_file.split(".")[-1]
        unique_schema = f"rav-test.{prefix}_{test_file}"
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
