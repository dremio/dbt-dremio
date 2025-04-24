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

from dbt.tests.util import write_file, run_dbt, run_dbt_and_capture

my_model_sql = """
select
    tested_column
from (
    select
      {sql_value} as tested_column
)
"""

test_my_model_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    given:
      - input: this
        rows:
          - {{ tested_column: {yaml_value} }}
    expect:
      rows:
        - {{ tested_column: {yaml_value} }}
"""

class TestDremioUnitTestingTypes(BaseUnitTestingTypes):
    # TODO: Revisit once we support unit testing with models that have dependencies
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "schema.yml": test_my_model_yml
        }

    @pytest.fixture
    def data_types(self):
        # https://docs.dremio.com/current/reference/sql/data-types/
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["'1'", "1"],
            ["cast('true' as boolean)", "true"],
            # ["1.0", "1.0"], # FIXME: Revisit, wrong fixture
            ["'string value'", "string value"],
            # ["cast(1.0 as numeric)", "1.0"], # FIXME: Revisit, wrong fixture
            ["cast(1 as bigint)", 1],
            ["cast('2019-01-01' as date)", "2019-01-01"],
            # ["cast('2013-11-03 00:00:00' as timestamp)", "2013-11-03 00:00:00"], #FIXME: Revisit, wrong fixture
            # ["st_geogpoint(75, 45)", "'st_geogpoint(75, 45)'"], # ?
            # arrays #FIXME: Revisit, wrong fixture
            # ["array['a','b','c']", "['a','b','c']"],
            # ["array[1,2,3]", "[1,2,3]"],
            # ["array[true,true,false]", "[true,true,false]"],
            # array of date #FIXME: Revisit, wrong fixture
            # ["array[date '2019-01-01']", "['2020-01-01']"],
            # ["array[date '2019-01-01']", "[]"],
            # ["array[date '2019-01-01']", "null"],
            # array of timestamp #FIXME: Revisit, wrong fixture
            # ["array[timestamp '2019-01-01']", "['2020-01-01']"],
            # ["array[timestamp '2019-01-01']", "[]"],
            # ["array[timestamp '2019-01-01']", "null"],
            # json
            # [
            #     """json '{"name": "Cooper", "forname": "Alice"}'""",
            #     """{"name": "Cooper", "forname": "Alice"}""",
            # ],
            # ["""json '{"name": "Cooper", "forname": "Alice"}'""", "{}"],
            # structs
            # [
            #     "struct('Isha' as name, 22 as age)",
            #     """'struct("Isha" as name, 22 as age)'""",
            # ],
            # [
            #     "struct('Kipketer' AS name, [23.2, 26.1, 27.3, 29.4] AS laps)",
            #     """'struct("Kipketer" AS name, [23.2, 26.1, 27.3, 29.4] AS laps)'""",
            # ],
            # struct of struct
            # [
            #     "struct(struct(1 as id, 'blue' as color) as my_struct)",
            #     """'struct(struct(1 as id, "blue" as color) as my_struct)'""",
            # ],
            # array of struct
            # [
            #     "[struct(st_geogpoint(75, 45) as my_point), struct(st_geogpoint(75, 35) as my_point)]",
            #     "['struct(st_geogpoint(75, 45) as my_point)', 'struct(st_geogpoint(75, 35) as my_point)']",
            # ],
        ]

    def test_unit_test_data_type(self, project, data_types):
        for sql_value, yaml_value in data_types:
            # Write parametrized type value to sql files
            write_file(
                my_model_sql.format(sql_value=sql_value),
                "models",
                "my_model.sql",
            )

            # Write parametrized type value to unit test yaml definition
            write_file(
                test_my_model_yml.format(yaml_value=yaml_value),
                "models",
                "schema.yml",
            )

            results = run_dbt(["run", "--select", "my_model"])
            assert len(results) == 1

            try:
                run_dbt(["test", "--select", "my_model"])
            except Exception:
                raise AssertionError(f"unit test failed when testing model with {sql_value}")


my_model_2_sql = """
select
    tested_column
from (
    select
      1 as tested_column
)
"""

test_my_model_2_yml = """
unit_tests:
  - name: test_my_model
    model: my_model_2
    given:
      - input: this
        rows:
          - {tested_column: 1}
          - {TESTED_COLUMN: 2}
          - {tested_colUmn: 3}
    expect:
      rows:
          - {tested_column: 1}
          - {TESTED_COLUMN: 2}
          - {tested_colUmn: 3}
"""

class TestDremioUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity):
    # FIXME: Revisit
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_2.sql": my_model_2_sql,
            "unit_tests_2.yml": test_my_model_2_yml,
        }

    def test_case_insensitivity(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        results = run_dbt(["test"])


test_my_model_3_yml = """
unit_tests:
  - name: test_invalid_input_column_name
    model: my_model_2
    given:
      - input: this
        rows:
          - {invalid_column_name: 1}
    expect:
      rows:
          - {tested_column: 1}
  - name: test_invalid_expect_column_name
    model: my_model_2
    given:
      - input: this
        rows:
          - {tested_column: 1}
    expect:
      rows:
          - {invalid_column_name: 1}
"""

class TestDremioUnitTestInvalidInput(BaseUnitTestInvalidInput):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_2.sql": my_model_2_sql,
            "unit_tests_3.yml": test_my_model_3_yml
        }

    def test_invalid_input(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        _, out = run_dbt_and_capture(
            ["test", "--select", "test_name:test_invalid_input_column_name"], expect_pass=False
        )
        assert (
            "Invalid column name: 'invalid_column_name' in unit test fixture for 'my_model_2'."
            in out
        )

        _, out = run_dbt_and_capture(
            ["test", "--select", "test_name:test_invalid_expect_column_name"], expect_pass=False
        )
        assert (
            "Invalid column name: 'invalid_column_name' in unit test fixture for expected output."
            in out
        )
