from dbt.tests.adapter.relations.test_changing_relation_type import (
    BaseChangeRelationTypeValidator,
)
from tests.fixtures.profiles import unique_schema, dbt_profile_data


class TestChangeRelationTypesDremio(BaseChangeRelationTypeValidator):
    pass
