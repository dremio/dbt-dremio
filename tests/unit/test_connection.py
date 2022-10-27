from dbt.adapters.dremio.api.rest.error import DremioRequestTimeoutException
import pytest
from dbt.tests.util import run_dbt
from unittest.mock import patch
from dbt.adapters.dremio.connections import DremioConnectionManager

from tests.unit.utils.fixtures import select_model_sql


class TestRetryConnections:
    @patch("dbt.adapters.dremio.api.handle.DremioHandle")
    @patch("dbt.contracts.connection.Connection")
    # When you nest patch decorators the mocks are passed in to the decorated function in bottom up order.
    def test_run_connection_retry_test(self, mocked_Connection, mocked_DremioHandle):
        mocked_DremioHandle.cursor.side_effect = DremioRequestTimeoutException

        DremioConnectionManager.open(connection=mocked_Connection)
        mocked_DremioHandle.cursor.assert_called()
