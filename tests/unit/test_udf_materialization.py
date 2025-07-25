import os
import re

MACRO_PATH = os.path.join(
    "dbt", "include", "dremio", "macros", "materializations", "udf.sql"
)


def test_udf_materialization_file_exists():
    """Test that the UDF materialization macro file exists."""
    assert os.path.isfile(MACRO_PATH)


def test_udf_materialization_contains_expected_sql():
    """Test that the UDF materialization contains expected SQL components."""
    with open(MACRO_PATH) as f:
        contents = f.read()

    # Basic structure
    assert "{% materialization udf" in contents
    assert "adapter='dremio'" in contents
    assert "CREATE OR REPLACE FUNCTION" in contents
    assert "return({'relations': [target_relation]})" in contents
    
    # Configuration handling
    assert "config.get('database'" in contents
    assert "config.get('schema'" in contents
    assert "config.get('parameter_list'" in contents
    assert "config.get('returns'" in contents
    
    # SQL construction
    assert "RETURNS {{ ret }}" in contents
    assert "{{ target }}({{ parameter_list }})" in contents
    assert "{{ sql }}" in contents


def test_udf_materialization_macro_structure():
    """Test the macro structure and Jinja2 syntax."""
    with open(MACRO_PATH) as f:
        contents = f.read()
    
    # Check proper Jinja2 block structure
    assert contents.count("{% materialization") == 1
    assert contents.count("{% endmaterialization %}") == 1
    assert contents.count("{%- set") >= 5  # identifier, target_relation, udf_database, udf_schema, parameter_list, ret, target, create_sql
    assert contents.count("{%- endset -%}") >= 1  # create_sql
    
    # Check statement execution
    assert "{% call statement('main')" in contents
    assert "{%- endcall %}" in contents


def test_udf_materialization_target_construction():
    """Test that the target name is properly constructed with database.schema.identifier."""
    with open(MACRO_PATH) as f:
        contents = f.read()
    
    # Check the target construction logic
    assert "udf_database ~ '.' ~ udf_schema ~ '.' ~ identifier" in contents
    assert "config.get('database'" in contents
    assert "config.get('schema'" in contents
    
    # Verify the pattern creates: database.schema.function_name
    target_pattern = r"set target = udf_database ~ '\.' ~ udf_schema ~ '\.' ~ identifier"
    assert re.search(target_pattern, contents, re.IGNORECASE)


def test_udf_materialization_sql_generation():
    """Test that the CREATE FUNCTION SQL is properly generated."""
    with open(MACRO_PATH) as f:
        contents = f.read()
    
    # Check the SQL template structure
    create_sql_block = re.search(
        r"{%- set create_sql -%}(.*?){%- endset -%}",
        contents,
        re.DOTALL
    )
    assert create_sql_block is not None
    
    sql_content = create_sql_block.group(1)
    assert "CREATE OR REPLACE FUNCTION" in sql_content
    assert "{{ target }}" in sql_content
    assert "{{ parameter_list }}" in sql_content
    assert "RETURNS {{ ret }}" in sql_content
    assert "{{ sql }}" in sql_content


def test_udf_materialization_validation():
    """Test that the macro includes proper validation."""
    with open(MACRO_PATH) as f:
        contents = f.read()
    
    # Check that validators are used for config values
    assert "validator=validation.any[basestring]" in contents
