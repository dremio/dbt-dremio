import pytest
from dbt.tests.adapter.python_model.test_python_model.py import (
    BasePythonModelTests,
    BasePythonIncrementalTests,
)


class TestPythonModelSnowflake(BasePythonModelTests):
    pass


class TestIncrementalSnowflake(BasePythonIncrementalTests):
    pass
