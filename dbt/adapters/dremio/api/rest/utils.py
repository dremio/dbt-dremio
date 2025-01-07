# Copyright (C) 2022 Dremio Corporation

# Copyright (c) 2019 Ryan Murray.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from .error import (
    DremioBadRequestException,
    DremioException,
    DremioNotFoundException,
    DremioPermissionException,
    DremioUnauthorizedException,
    DremioAlreadyExistsException,
    DremioRequestTimeoutException,
    DremioTooManyRequestsException,
    DremioInternalServerException,
    DremioServiceUnavailableException,
    DremioGatewayTimeoutException,
)

import requests
import json as jsonlib
from requests.exceptions import HTTPError

from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("dremio")

session = requests.Session()


def _get(url, request_headers, details="", ssl_verify=True):
    response = session.get(url, headers=request_headers, verify=ssl_verify)
    return _check_error(response, details)


def _post(
        url,
        request_headers=None,
        json=None,
        details="",
        ssl_verify=True,
        timeout=None,
):
    if isinstance(json, str):
        json = jsonlib.loads(json)
    response = session.post(
        url,
        headers=request_headers,
        timeout=timeout,
        verify=ssl_verify,
        json=json,
    )
    return _check_error(response, details)


def _put(url, request_headers, json=None, details="", ssl_verify=True):
    response = session.put(
        url, headers=request_headers, verify=ssl_verify, json=json
    )
    return _check_error(response, details)


def _delete(url, request_headers, details="", ssl_verify=True):
    response = session.delete(url, headers=request_headers, verify=ssl_verify)
    return _check_error(response, details)


def _raise_for_status(self):
    """Raises stored :class:`HTTPError`, if one occurred. Copy from requests request.raise_for_status()"""

    http_error_msg = ""
    if isinstance(self.reason, bytes):
        try:
            reason = self.reason.decode("utf-8")
        except UnicodeDecodeError:
            reason = self.reason.decode("iso-8859-1")
    else:
        reason = self.reason

    if 400 <= self.status_code < 500:
        http_error_msg = "%s Client Error: %s for url: %s" % (
            self.status_code,
            reason,
            self.url,
        )

    elif 500 <= self.status_code < 600:
        http_error_msg = "%s Server Error: %s for url: %s" % (
            self.status_code,
            reason,
            self.url,
        )

    if http_error_msg:
        return (
            HTTPError(http_error_msg, response=self),
            self.status_code,
            reason,
        )
    else:
        return None, self.status_code, reason


def _check_error(response, details=""):
    error, code, reason = _raise_for_status(response)
    if not error:
        try:
            data = response.json()
            return data
        except:  # NOQA
            return response.text
    if code == 400:
        raise DremioBadRequestException("Bad request:" + details, error,
                                        response)
    if code == 401:
        raise DremioUnauthorizedException("Unauthorized:" + details, error,
                                          response)
    if code == 403:
        raise DremioPermissionException("No permission:" + details, error,
                                        response)
    if code == 404:
        raise DremioNotFoundException("Not found:" + details, error, response)
    if code == 408:
        raise DremioRequestTimeoutException(
            "Request timeout:" + details, error, response
        )
    if code == 409:
        raise DremioAlreadyExistsException("Already exists:" + details, error,
                                           response)
    if code == 429:
        raise DremioTooManyRequestsException(
            "Too many requests:" + details, error, response
        )
    if code == 500:
        raise DremioInternalServerException(
            "Internal server error:" + details, error, response
        )
    if code == 503:
        raise DremioServiceUnavailableException(
            "Service unavailable:" + details, error, response
        )
    if code == 504:
        raise DremioGatewayTimeoutException(
            "Gateway Timeout:" + details, error, response
        )
    raise DremioException("Unknown error", error)
