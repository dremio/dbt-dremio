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
            ["cast('13:45:30' as time)", "'13:45:30.000'"],
            ["cast('2013-11-03 00:00:00' as timestamp)", "'2013-11-03 00:00:00.000'"],

            # Arrays / Lists
            ["array['a','b','c']", "['a','b','c']"],
            ["array[1,2,3]", "[1,2,3]"],
            ["array[true,true,false]", "[true,true,false]"],
            ["array[date '2019-01-01']", "['2019-01-01']"],
            ["array[timestamp '2019-01-01']", "['2019-01-01 00:00:00.000']"],

            # Binary
            ["cast('abc' as binary)", "YWJj"],

            # Intervals
            ["interval '2' year", "\"'2' year\""],
            ["interval '3' month", "\"'3' month\""],
            ["interval '5' day", "\"'5' day\""],
            ["interval '12' hour", "\"'12' hour\""],
            ["interval '30' minute", "\"'30' minute\""],
            ["interval '45' second", "\"'45' second\""],

            # Struct
            ["convert_from('{a:1}', 'json')", "\"convert_from('{a:1}', 'json')\""],
            ["convert_from('[1,2,3]', 'json')", "\"convert_from('[1,2,3]', 'json')\""],
            ["convert_from('{x:1, y:2}', 'json')", "\"convert_from('{x:1, y:2}', 'json')\""],
        ]

class TestDremioUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity):
    pass

class TestDremioUnitTestInvalidInput(BaseUnitTestInvalidInput):
    pass
