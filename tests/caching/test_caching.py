import pytest
from tests.utils.util import BUCKET
from dbt.tests.adapter.caching.test_caching import (
    TestNoPopulateCache,
    BaseCachingSelectedSchemaOnly,
    BaseCachingLowercaseModel,
    BaseCachingUppercaseModel,
    model_sql,
)


# This ensures the schema works with our datalake
@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    test_file = request.module.__name__
    # We only want the last part of the name
    test_file = test_file.split(".")[-1]
    unique_schema = f"{BUCKET}.{prefix}_{test_file}"
    return unique_schema


# Override this fixture to set root_path=schema
@pytest.fixture(scope="class")
def dbt_profile_data(unique_schema, dbt_profile_target, profiles_config_update):
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
    target["root_path"] = f"{unique_schema}"
    profile["test"]["outputs"]["default"] = target

    if profiles_config_update:
        profile.update(profiles_config_update)
    return profile


class TestNoPopulateCacheDremio(TestNoPopulateCache):
    pass


class TestCachingLowerCaseModelDremio(BaseCachingLowercaseModel):
    pass


@pytest.mark.skip(
    reason="Dremio does not support case-sensitive data file names or table names."
)
class TestCachingUppercaseModelDremio(BaseCachingUppercaseModel):
    pass


class TestCachingSelectedSchemaOnlyDremio(BaseCachingSelectedSchemaOnly):
    pass
