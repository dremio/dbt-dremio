import pytest
import yaml
from dbt.tests.util import (
    run_dbt,
    read_file,
    write_file,
)

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
            {"object_storage_path": 'dbtdremios3."test.schema"'},
            project.profiles_dir,
            "profiles.yml",
        )

        run_dbt(["run"])
