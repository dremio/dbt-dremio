import pytest
import yaml
from dbt.tests.util import (
    run_dbt,
    read_file,
    write_file,
)

from tests.utils.util import BUCKET

test_schema_parsing = """
{{
  config(
    materialized = "table"
  )
}}
select * from Samples."samples.dremio.com"."NYC-taxi-trips-iceberg" limit 10
"""


class TestSchemaParsingDremio:
    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        # We only want the last part of the name
        test_file = test_file.split(".")[-1]
        unique_schema = f"{BUCKET}.{prefix}_{test_file}"
        return unique_schema

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_schema_parsing.sql": test_schema_parsing,
        }

    def update_config_file(self, updates, *paths):
        current_yaml = read_file(*paths)
        config = yaml.safe_load(current_yaml)
        config["test"]["outputs"]["default"].update(updates)
        new_yaml = yaml.safe_dump(config)
        write_file(new_yaml, *paths)

    def test_schema_with_dots(self, project):
        self.update_config_file(
            {"root_path": 'dbtdremios3."test.schema"'},
            project.profiles_dir,
            "profiles.yml",
        )

        run_dbt(["run"])
