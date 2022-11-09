import pytest
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.ephemeral.test_ephemeral import BaseEphemeralMulti
from tests.functional.adapter.utils.test_utils import DATALAKE
from dbt.tests.util import run_dbt
from tests.fixtures.profiles import unique_schema, dbt_profile_data


class TestEphemeralDremio(BaseEphemeral):
    pass
