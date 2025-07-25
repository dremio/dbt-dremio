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
    assert "create or replace function" in contents
    assert "return({'relations': [target_relation]})" in contents

    # Configuration handling
    assert "config.get('database'" in contents
    assert "config.get('schema'" in contents
    assert "config.get('parameter_list'" in contents
    assert "config.get('returns'" in contents

    # SQL construction
    assert "returns {{ ret }}" in contents
    assert "{{ target_relation }}({{ parameter_list }})" in contents
    assert "{{ sql }}" in contents


def test_udf_materialization_macro_structure():
    """Test the macro structure and Jinja2 syntax."""
    with open(MACRO_PATH) as f:
        contents = f.read()

    # Check proper Jinja2 block structure
    assert contents.count("{% materialization") == 1
    assert contents.count("{% endmaterialization %}") == 1
    assert (
        contents.count("{%- set") >= 5
    )  # identifier, udf_database, udf_schema, target_relation, parameter_list, ret, old_relation
    # No longer using create_sql block - SQL is directly in statement call

    # Check statement execution
    assert "{% call statement('main')" in contents
    assert "{%- endcall %}" in contents


def test_udf_materialization_target_construction():
    """Test that the target relation is properly constructed using api.Relation.create."""
    with open(MACRO_PATH) as f:
        contents = f.read()

    # Check the new relation construction logic using api.Relation.create
    assert "api.Relation.create(" in contents
    assert "identifier=identifier" in contents
    assert "schema=udf_schema" in contents
    assert "database=udf_database" in contents
    assert "config.get('database'" in contents
    assert "config.get('schema'" in contents


def test_udf_materialization_sql_generation():
    """Test that the CREATE FUNCTION SQL is properly generated."""
    with open(MACRO_PATH) as f:
        contents = f.read()

    # Check that SQL is properly structured within the statement call
    statement_block = re.search(
        r"{% call statement\('main'\) -%}(.*?){%- endcall %}", contents, re.DOTALL
    )
    assert statement_block is not None

    sql_content = statement_block.group(1)
    assert "create or replace function" in sql_content
    assert "{{ target_relation }}" in sql_content
    assert "{{ parameter_list }}" in sql_content
    assert "returns {{ ret }}" in sql_content
    assert "{{ sql }}" in sql_content


def test_udf_materialization_validation():
    """Test that the macro includes proper validation."""
    with open(MACRO_PATH) as f:
        contents = f.read()

    # Check that validators are used for config values
    assert "validator=validation.any[basestring]" in contents
