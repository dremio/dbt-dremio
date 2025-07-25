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
from dbt.tests.util import run_dbt, check_result_nodes_by_name


# Basic UDF with simple arithmetic
basic_udf_sql = """
{{ config(
    materialized='udf',
    parameter_list='x INT, y INT',
    returns='INT'
) }}

RETURN x + y
"""

# UDF with no parameters - should fail validation
no_params_udf_sql = """
{{ config(
    materialized='udf',
    parameter_list='',
    returns='VARCHAR'
) }}

RETURN 'Hello World'
"""

# UDF with no parameter_list config - should fail
no_param_list_config_sql = """
{{ config(
    materialized='udf',
    returns='VARCHAR'
) }}

RETURN 'Hello World'
"""

# UDF with complex types
complex_udf_sql = """
{{ config(
    materialized='udf',
    parameter_list='name VARCHAR, age INT, salary DECIMAL(10,2)',
    returns='VARCHAR'
) }}

RETURN CONCAT(name, ' is ', CAST(age AS VARCHAR), ' years old and earns ', CAST(salary AS VARCHAR))
"""

# UDF with custom database configuration (uses existing test schema)
custom_location_udf_sql = """
{{ config(
    materialized='udf',
    database='dbt_test',
    schema='{{ target.schema }}',
    parameter_list='x DOUBLE',
    returns='DOUBLE'
) }}

RETURN x * x
"""

# Model that uses a UDF
downstream_model_sql = """
{{ config(materialized='view') }}

SELECT 
    {{ ref('basic_udf') }}(10, 20) as udf_output
FROM (SELECT 1 as dummy) t
"""

# Invalid UDF - missing returns config
invalid_udf_no_returns_sql = """
{{ config(
    materialized='udf',
    parameter_list='x INT'
) }}

RETURN x * 2
"""

# Invalid UDF - missing parameter_list config
invalid_udf_no_params_sql = """
{{ config(
    materialized='udf',
    returns='INT'
) }}

RETURN 42
"""


class TestBasicUDFMaterialization:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "basic_udf.sql": basic_udf_sql,
            "downstream_model.sql": downstream_model_sql,
        }

    def test_basic_udf_creation(self, project):
        # First compile to see what's generated
        compile_results = run_dbt(["compile"])
        
        # Read the compiled SQL to debug
        import os
        compiled_path = os.path.join(project.project_root, "target", "compiled", "test", "models", "downstream_model.sql")
        if os.path.exists(compiled_path):
            with open(compiled_path, 'r') as f:
                compiled_sql = f.read()
                print(f"\n\nCOMPILED downstream_model.sql:\n{compiled_sql}\n\n")
        
        # Run both models  
        results = run_dbt(["run"])
        assert len(results) == 2
        check_result_nodes_by_name(results, ["basic_udf", "downstream_model"])

        # Test docs generation
        results = run_dbt(["docs", "generate"])
        assert results is not None


class TestUDFParameterVariations:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "complex_udf.sql": complex_udf_sql,
        }

    def test_udf_parameter_variations(self, project):
        # Run complex UDF model
        results = run_dbt(["run"])
        assert len(results) == 1
        check_result_nodes_by_name(results, ["complex_udf"])

        # Verify they can be re-run (CREATE OR REPLACE)
        results = run_dbt(["run"])
        assert len(results) == 1


class TestUDFWithCustomLocation:
    @pytest.fixture(scope="class")
    def models(self, unique_schema):
        # Use the unique_schema fixture to dynamically set the schema
        custom_udf_with_schema = """
{{ config(
    materialized='udf',
    database='dbt_test',
    schema='""" + unique_schema + """',
    parameter_list='x DOUBLE',
    returns='DOUBLE'
) }}

RETURN x * x
"""
        return {
            "custom_location_udf.sql": custom_udf_with_schema,
        }

    def test_udf_with_custom_location(self, project):
        # Run the UDF with custom database/schema configuration
        results = run_dbt(["run", "--select", "custom_location_udf"])
        assert len(results) == 1
        check_result_nodes_by_name(results, ["custom_location_udf"])


class TestUDFErrorHandling:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid_udf_no_returns.sql": invalid_udf_no_returns_sql,
            "invalid_udf_no_params.sql": invalid_udf_no_params_sql,
            "no_params_udf.sql": no_params_udf_sql,
            "no_param_list_config.sql": no_param_list_config_sql,
        }

    def test_missing_returns_config(self, project):
        # This should fail due to missing returns config
        results = run_dbt(["run", "--select", "invalid_udf_no_returns"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"
        assert "UDF materialization requires 'returns' configuration" in results[0].message

    def test_missing_parameter_list_config(self, project):
        # This should fail due to missing parameter_list config
        results = run_dbt(["run", "--select", "invalid_udf_no_params"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"
        assert "UDF materialization requires 'parameter_list' configuration" in results[0].message

    def test_empty_parameter_list(self, project):
        # This should fail due to empty parameter_list
        results = run_dbt(["run", "--select", "no_params_udf"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"
        assert "UDF materialization requires 'parameter_list' configuration" in results[0].message

    def test_no_parameter_list_config_key(self, project):
        # This should fail due to no parameter_list config key
        results = run_dbt(["run", "--select", "no_param_list_config"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"
        assert "UDF materialization requires 'parameter_list' configuration" in results[0].message


class TestUDFDownstreamUsage:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "add_numbers.sql": """
                {{ config(
                    materialized='udf',
                    parameter_list='a INT, b INT',
                    returns='INT'
                ) }}
                
                RETURN a + b
            """,
            "simple_view.sql": """
                {{ config(materialized='view') }}
                
                SELECT 
                    {{ ref('add_numbers') }}(5, 3) as sum_output
            """,
            "multiply_numbers.sql": """
                {{ config(
                    materialized='udf',
                    parameter_list='x INT, y INT',
                    returns='INT'
                ) }}
                
                RETURN x * y
            """,
            "calculations_view.sql": """
                {{ config(materialized='view') }}
                
                SELECT 
                    {{ ref('add_numbers') }}(5, 3) as sum_result,
                    {{ ref('multiply_numbers') }}(4, 6) as product_result,
                    {{ ref('add_numbers') }}(
                        {{ ref('multiply_numbers') }}(2, 3),
                        10
                    ) as nested_result
            """,
            "final_report.sql": """
                {{ config(
                    materialized='view'
                ) }}
                
                SELECT 
                    sum_result,
                    product_result,
                    nested_result,
                    {{ ref('add_numbers') }}(sum_result, product_result) as total
                FROM {{ ref('calculations_view') }}
            """,
        }

    def test_udf_downstream_usage(self, project):
        # Run all models
        results = run_dbt(["run"])
        assert len(results) == 5
        check_result_nodes_by_name(
            results, ["add_numbers", "multiply_numbers", "simple_view", "calculations_view", "final_report"]
        )

        # Test selective execution
        results = run_dbt(["run", "--select", "+final_report"])
        assert len(results) == 4

        # Test that UDFs are properly tracked in the manifest
        results = run_dbt(["ls", "--select", "add_numbers+"])
        assert "calculations_view" in str(results)
        assert "final_report" in str(results)