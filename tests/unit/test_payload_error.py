from unittest import mock, TestCase
from dbt.adapters.dremio.api.cursor import DremioCursor
from dbt.adapters.dremio.api.parameters import Parameters
from dbt.adapters.dremio.api.authentication import DremioAuthentication


class TestPayloadErrorMessage(TestCase):
    @mock.patch("dbt.adapters.dremio.api.cursor.job_status")
    def test_payload_error(self, mocked_job_status):
        dremio_cursor_object = DremioCursor(
            Parameters("base_url", DremioAuthentication)
        )
        mocked_job_status.return_value = {
            "jobState": "FAILED",
            "errorMessage": "ERROR: Expected error message",
        }
        with self.assertRaises(Exception, msg="ERROR: Expected error message"):
            dremio_cursor_object._populate_rowcount()
