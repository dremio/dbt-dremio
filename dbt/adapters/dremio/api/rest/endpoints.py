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

from dbt.adapters.dremio.api.authentication import DremioPatAuthentication
from dbt.adapters.dremio.api.parameters import Parameters
from dbt.adapters.dremio.api.rest.url_builder import UrlBuilder

from dbt.events import AdapterLogger

logger = AdapterLogger("dremio")


def _get(url, request_headers, details="", ssl_verify=True):
    response = requests.get(url, headers=request_headers, verify=ssl_verify)
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
    response = requests.post(
        url,
        headers=request_headers,
        timeout=timeout,
        verify=ssl_verify,
        json=json,
    )
    return _check_error(response, details)


def _delete(url, request_headers, details="", ssl_verify=True):
    response = requests.delete(url, headers=request_headers, verify=ssl_verify)
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
        raise DremioBadRequestException("Bad request:" + details, error, response)
    if code == 401:
        raise DremioUnauthorizedException("Unauthorized:" + details, error, response)
    if code == 403:
        raise DremioPermissionException("No permission:" + details, error, response)
    if code == 404:
        raise DremioNotFoundException("Not found:" + details, error, response)
    if code == 408:
        raise DremioRequestTimeoutException(
            "Request timeout:" + details, error, response
        )
    if code == 409:
        raise DremioAlreadyExistsException("Already exists:" + details, error, response)
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


def login(api_parameters: Parameters, timeout=10):

    if isinstance(api_parameters.authentication, DremioPatAuthentication):
        return api_parameters

    url = UrlBuilder.login_url(api_parameters)
    response = _post(
        url,
        json={
            "userName": api_parameters.authentication.username,
            "password": api_parameters.authentication.password,
        },
        timeout=timeout,
        ssl_verify=api_parameters.authentication.verify_ssl,
    )

    api_parameters.authentication.token = response["token"]

    return api_parameters


def sql_endpoint(api_parameters: Parameters, query, context=None):
    url = UrlBuilder.sql_url(api_parameters)
    return _post(
        url,
        api_parameters.authentication.get_headers(),
        ssl_verify=api_parameters.authentication.verify_ssl,
        json={"sql": query, "context": context},
    )


def job_status(api_parameters: Parameters, job_id):
    url = UrlBuilder.job_status_url(api_parameters, job_id)
    return _get(
        url,
        api_parameters.authentication.get_headers(),
        ssl_verify=api_parameters.authentication.verify_ssl,
    )


def job_cancel_api(api_parameters: Parameters, job_id):
    url = UrlBuilder.job_cancel_url(api_parameters, job_id)
    return _post(
        url,
        api_parameters.authentication.get_headers(),
        json=None,
        ssl_verify=api_parameters.authentication.verify_ssl,
    )


def job_results(api_parameters: Parameters, job_id, offset=0, limit=100):
    url = UrlBuilder.job_results_url(
        api_parameters,
        job_id,
        offset,
        limit,
    )
    return _get(
        url,
        api_parameters.authentication.get_headers(),
        ssl_verify=api_parameters.authentication.verify_ssl,
    )


def create_catalog_api(api_parameters, json):
    url = UrlBuilder.catalog_url(api_parameters)
    return _post(
        url,
        api_parameters.authentication.get_headers(),
        json=json,
        ssl_verify=api_parameters.authentication.verify_ssl,
    )


def get_catalog_item(api_parameters, catalog_id=None, catalog_path=None):
    if catalog_id is None and catalog_path is None:
        raise TypeError("both id and path can't be None for a catalog_item call")

    # Will use path if both id and path are specified
    if catalog_path:
        url = UrlBuilder.catalog_item_by_path_url(
            api_parameters,
            catalog_path,
        )
    else:
        url = UrlBuilder.catalog_item_by_id_url(
            api_parameters,
            catalog_id,
        )
    return _get(
        url,
        api_parameters.authentication.get_headers(),
        ssl_verify=api_parameters.authentication.verify_ssl,
    )


def delete_catalog(api_parameters, cid):
    url = UrlBuilder.delete_catalog_url(api_parameters, cid)
    return _delete(
        url,
        api_parameters.authentication.get_headers(),
        ssl_verify=api_parameters.authentication.verify_ssl,
    )
