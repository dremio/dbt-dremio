# Copyright (C) 2022 Dremio Corporation

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes
from dbt.tests.adapter.unit_testing.test_case_insensitivity import (
    BaseUnitTestCaseInsensivity,
)
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput

from dbt.tests.util import write_file, run_dbt

my_model_sql = """
select
    tested_column from {{ ref('my_upstream_model')}}
"""

my_upstream_model_sql = """
select
  {sql_value} as tested_column
"""

test_my_model_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    given:
      - input: ref('my_upstream_model')
        rows:
          - {{ tested_column: {yaml_value} }}
    expect:
      rows:
        - {{ tested_column: {yaml_value} }}
"""

class TestDremioUnitTestingTypes(BaseUnitTestingTypes):
    @pytest.fixture
    def data_types(self):
        # https://docs.dremio.com/current/reference/sql/data-types/
        # sql_value, yaml_value
        return [
            # Numeric Types
            ["1", "1"],
            ["'1'", "1"],
            ["1.0", "1.0"],
            ["cast(1 as bigint)", 1],
            ["cast(1.0 as numeric)", "1.0"],
            ["cast('3.14' as float)", "3.14"],
            ["cast('3.1415926535' as double)", "3.1415926535"],
            ["cast('12345.67' as decimal(7,2))", "12345.67"],

            # Boolean
            ["cast('true' as boolean)", "true"],
            ["cast(0 as boolean)", "false"],

            # String Types
            ["'string value'", "string value"],
            ["cast('abc' as char(5))", "abc  "],

            # Date/Time
            ["cast('2019-01-01' as date)", "2019-01-01"],
            # ["cast('13:45:30' as time)", "13:45:30.000"],
            # ["cast('2013-11-03 00:00:00' as timestamp)", "2013-11-03 00:00:00.000"],

            # Arrays / Lists
            ["array['a','b','c']", "['a','b','c']"],
            ["array[1,2,3]", "[1,2,3]"],
            ["array[true,true,false]", "[true,true,false]"],
            ["array[date '2019-01-01']", "['2019-01-01']"],
            ["array[timestamp '2019-01-01']", "['2019-01-01 00:00:00.000']"],

            # # Binary
            # ["cast('abc' as binary)", "YWJj"],

            # # Intervals
            # ["interval '2' year", "+002-00"],
            # ["interval '3' month", "3 months"],
            # ["interval '5' day", "5 days"],
            # ["interval '12' hour", "12 hours"],
            # ["interval '30' minute", "30 minutes"],
            # ["interval '45' second", "45 seconds"],

            # # JSON Complex Types via CONVERT_FROM
            # ["convert_from('{\"a\": 1}', 'json')", "{'a': 1}"],
            # ["convert_from('[1,2,3]', 'json')", "[1, 2, 3]"],
            # ["convert_from('{\"x\":1, \"y\":2}', 'json')", "{'x': 1, 'y': 2}"],
        ]
    
    def test_unit_test_data_type(self, project, data_types):
        for sql_value, yaml_value in data_types:
            # Write parametrized type value to sql files
            write_file(
                my_upstream_model_sql.format(sql_value=sql_value),
                "models",
                "my_upstream_model.sql",
            )

            # Write parametrized type value to unit test yaml definition
            write_file(
                test_my_model_yml.format(yaml_value=yaml_value),
                "models",
                "schema.yml",
            )

            results = run_dbt(["run", "--select", "my_upstream_model"])
            assert len(results) == 1

            try:
                run_dbt(["test", "--debug", "--select", "my_model"])
            except Exception:
                raise AssertionError(f"unit test failed when testing model with {sql_value}")

class TestDremioUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity):
    pass

class TestDremioUnitTestInvalidInput(BaseUnitTestInvalidInput):
    pass
