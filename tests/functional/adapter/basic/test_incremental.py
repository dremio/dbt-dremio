import pytest
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
)
from tests.fixtures.profiles import unique_schema, dbt_profile_data


class TestIncrementalDremio(BaseIncremental):
    pass


class TestBaseIncrementalNotSchemaChange(BaseIncrementalNotSchemaChange):
    pass
