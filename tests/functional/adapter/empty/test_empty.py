import pytest
from dbt.tests.adapter.empty.test_empty import BaseTestEmpty
from dbt.tests.adapter.empty import _models
from tests.fixtures.profiles import unique_schema, dbt_profile_data
from dbt.tests.util import run_dbt

schema_sources_yml = """
sources:
  - name: seed_sources
    database: "dbt_test_source"
    schema: "{{ target.root_path }}"
    tables:
      - name: raw_source
"""


class TestDremioEmpty(BaseTestEmpty):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"+twin_strategy": "allow"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_input.sql": _models.model_input_sql,
            "ephemeral_model_input.sql": _models.ephemeral_model_input_sql,
            "model.sql": _models.model_sql,
            "sources.yml": schema_sources_yml,
        }

    def test_run_with_empty(self, project):
        # create source from seed
        run_dbt(["seed"])

        # run without empty - 3 expected rows in output - 1 from each input
        # run_dbt(["run"])
        # self.assert_row_count(project, "model", 3)

        # run with empty - 0 expected rows in output
        run_dbt(["run", "--empty"])
        self.assert_row_count(project, "model", 0)
