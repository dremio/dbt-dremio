import pytest
from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseQueryComments,
    BaseMacroQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseNullQueryComments,
    BaseEmptyQueryComments,
)
from tests.fixtures.profiles import unique_schema, dbt_profile_data


class TestQueryCommentsDremio(BaseQueryComments):
    pass


class TestMacroQueryCommentsDremio(BaseMacroQueryComments):
    pass


class TestMacroArgsQueryCommentsDremio(BaseMacroArgsQueryComments):
    pass


class TestMacroInvalidQueryCommentsDremio(BaseMacroInvalidQueryComments):
    pass


class TestNullQueryCommentsDremio(BaseNullQueryComments):
    pass


class TestEmptyQueryCommentsDremio(BaseEmptyQueryComments):
    pass
