import datetime
import os
from unittest import mock
import pytest

from dbt.tests.adapter.sample_mode.test_sample_mode import (
    BaseSampleModeTest,
)
from dbt.tests.util import run_dbt
from tests.utils.util import BUCKET, relation_from_name
from pprint import pformat

# Use UTC time to match dbt-core's sample mode window calculation
now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
twelve_hours_ago = now - datetime.timedelta(hours=12)
two_days_ago = now - datetime.timedelta(days=2)

_input_model_sql = f"""
{{{{ config(materialized='table', event_time='event_time') }}}}
select 1 as id, cast('{two_days_ago.strftime('%Y-%m-%d %H:%M:%S')}' as timestamp) as event_time
UNION ALL
select 2 as id, cast('{twelve_hours_ago.strftime('%Y-%m-%d %H:%M:%S')}' as timestamp) as event_time
UNION ALL
select 3 as id, cast('{now.strftime('%Y-%m-%d %H:%M:%S')}' as timestamp) as event_time
"""

class TestDremioSampleMode(BaseSampleModeTest):
    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        """
        This is the SQL that defines the input model to be sampled, including any {{ config(..) }}.
        event_time is a required configuration of this input
        """
        return _input_model_sql
    
    # Override this fixture to set the proper root_path
    @pytest.fixture(scope="class")
    def dbt_profile_data(
        self, unique_schema, dbt_profile_target, profiles_config_update
    ):
        profile = {
            "test": {
                "outputs": {
                    "default": {},
                },
                "target": "default",
            },
        }
        target = dbt_profile_target
        target["schema"] = unique_schema
        target["root_path"] = f"{BUCKET}.{unique_schema}"
        profile["test"]["outputs"]["default"] = target

        if profiles_config_update:
            profile.update(profiles_config_update)
        return profile

    # Pasting the original function here so it uses our relation_from_name
    def assert_row_count(self, project, relation_name: str, expected_row_count: int):
        relation = relation_from_name(project.adapter, relation_name)
        result = project.run_sql(f"select * from {relation}", fetch="all")

        assert len(result) == expected_row_count, f"{relation_name}:{pformat(result)}"

    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_SAMPLE_MODE": "True"})
    def test_sample_mode(self, project) -> None:
        _ = run_dbt(["run"])
        self.assert_row_count(
            project=project,
            relation_name="model_that_samples_input_sql",
            expected_row_count=3,
        )

        _ = run_dbt(["run", "--sample=1 day"])
        self.assert_row_count(
            project=project,
            relation_name="model_that_samples_input_sql",
            expected_row_count=2,
        )
