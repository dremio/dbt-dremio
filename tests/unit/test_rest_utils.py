# Copyright (C) 2022 Dremio Corporation
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from unittest.mock import MagicMock

import pytest

from dbt.adapters.dremio.api.rest.error import (
    DremioAlreadyExistsException,
    DremioBadRequestException,
)
from dbt.adapters.dremio.api.rest.utils import _check_error


_REASON_BY_STATUS = {400: "Bad Request", 409: "Conflict"}


def _make_response(status_code, body_text):
    response = MagicMock()
    response.status_code = status_code
    response.reason = _REASON_BY_STATUS.get(status_code, "Bad Request")
    response.url = "https://api.dremio.cloud/v0/projects/test/catalog"
    response.text = body_text
    return response


class TestCheckErrorAlreadyExists:
    def test_cloud_400_already_exists_routes_to_already_exists_exception(self):
        body = json.dumps(
            {
                "errorMessage": (
                    "Unable to create folder staging on source mySource. "
                    "An object already exists with that name."
                ),
                "moreInfo": "",
            }
        )
        response = _make_response(400, body)
        with pytest.raises(DremioAlreadyExistsException):
            _check_error(response)

    def test_400_other_message_still_raises_bad_request(self):
        body = json.dumps({"errorMessage": "Some other 400 error", "moreInfo": ""})
        response = _make_response(400, body)
        with pytest.raises(DremioBadRequestException):
            _check_error(response)

    def test_409_still_raises_already_exists(self):
        body = json.dumps({"errorMessage": "Already exists", "moreInfo": ""})
        response = _make_response(409, body)
        with pytest.raises(DremioAlreadyExistsException):
            _check_error(response)
