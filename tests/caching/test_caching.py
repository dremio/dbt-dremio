import pytest
from tests.utils.util import BUCKET
from dbt.tests.adapter.caching.test_caching import (
    TestNoPopulateCache,
    BaseCachingSelectedSchemaOnly,
    BaseCachingLowercaseModel,
    BaseCachingUppercaseModel,
    model_sql,
)

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
