from dbt.adapters.dremio.api.rest.error import DremioRequestTimeoutException
import pytest
from unittest.mock import patch
from dbt.adapters.dremio.connections import DremioConnectionManager
from requests.exceptions import HTTPError
from dbt.exceptions import FailedToConnectException


class TestRetryConnection:
    @patch("dbt.adapters.dremio.api.rest.endpoints._post")
    @patch("dbt.contracts.connection.Connection")
    # When you nest patch decorators the mocks are passed in to the decorated function in bottom up order.
    def test_connection_retry(
        self,
        mocked_Connection,
        mocked_post,
    ):

        mocked_post.side_effect = DremioRequestTimeoutException(
            msg="Request timeout: Test", original_exception="408 original exception"
        )

        with pytest.raises(FailedToConnectException) as exception_info:
            DremioConnectionManager.open(connection=mocked_Connection)

        # Post will be called once and then DEFAULT_TRIES more times.
        assert (
            mocked_post.call_count
            == DremioConnectionManager.DEFAULT_CONNECTION_RETRIES + 1
        )
