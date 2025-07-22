import os

MACRO_PATH = os.path.join(
    "dbt", "include", "dremio", "macros", "materializations", "udf.sql"
)


def test_udf_materialization_file_exists():
    assert os.path.isfile(MACRO_PATH)


def test_udf_materialization_contains_expected_sql():
    with open(MACRO_PATH) as f:
        contents = f.read()

    assert "{% materialization udf" in contents
    assert "CREATE OR REPLACE FUNCTION" in contents
    assert "return({'relations': []})" in contents
