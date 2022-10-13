# Copyright (C) 2022 Dremio Corporation 
#
# Copyright (c) 2019 Ryan Murray.
#
# Licensed under the Apache License, Version 2.0 (the "License"); 
# you may not use this file except in compliance with the License. 
# You may obtain a copy of the License at 
# http://www.apache.org/licenses/LICENSE-2.0 
# Unless required by applicable law or agreed to in writing, software 
# distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
# See the License for the specific language governing permissions and 
# limitations under the License.

from asyncio.log import logger
import requests
import json as jsonlib
from requests.exceptions import HTTPError
from urllib.parse import quote

from dbt.adapters.dremio.api.parameters import Parameters
from dbt.adapters.dremio.api.url_builder import UrlBuilder

from dbt.events import AdapterLogger
logger = AdapterLogger("dremio")

from .error import (
    DremioBadRequestException,
    DremioException,
    DremioNotFoundException,
    DremioPermissionException,
    DremioUnauthorizedException,
    DremioAlreadyExistsException
)

def _get(url, request_headers, details="", ssl_verify=True):
    r = requests.get(url, headers=request_headers, verify=ssl_verify)
    return _check_error(r, details)

def _post(url, request_headers, json=None, details="", ssl_verify=True):
    if isinstance(json, str):
        json = jsonlib.loads(json)
    r = requests.post(url, headers=request_headers, verify=ssl_verify, json=json)
    return _check_error(r, details)

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
        http_error_msg = u"%s Client Error: %s for url: %s" % (self.status_code, reason, self.url)

    elif 500 <= self.status_code < 600:
        http_error_msg = u"%s Server Error: %s for url: %s" % (self.status_code, reason, self.url)

    if http_error_msg:
        return HTTPError(http_error_msg, response=self), self.status_code, reason
    else:
        return None, self.status_code, reason

def _check_error(r, details=""):
    error, code, _ = _raise_for_status(r)
    if not error:
        try:
            data = r.json()
            return data
        except:  # NOQA
            return r.text
    if code == 400:
        raise DremioBadRequestException("Bad request:" + details, error, r)
    if code == 401:
        raise DremioUnauthorizedException("Unauthorized:" + details, error, r)
    if code == 403:
        raise DremioPermissionException("No permission:" + details, error, r)
    if code == 404:
        raise DremioNotFoundException("Not found:" + details, error, r)
    if code == 409:
        raise DremioAlreadyExistsException("Already exists:" + details, error, r)
    raise DremioException("Unknown error", error)

def catalog_item(token, base_url, cid=None, path=None, ssl_verify=True):
    """fetch a specific catalog item by id or by path

    https://docs.dremio.com/rest-api/catalog/get-catalog-id.html
    https://docs.dremio.com/rest-api/catalog/get-catalog-path.html

    :param token: auth token from previous login attempt
    :param base_url: base Dremio url
    :param cid: unique dremio id for resource
    :param path: list ['space', 'folder', 'vds']
    :param ssl_verify: ignore ssl errors if False
    :return: json of resource
    """
    if cid is None and path is None:
        raise TypeError("both id and path can't be None for a catalog_item call")
    idpath = (cid if cid else "") + ", " + (".".join(path) if path else "")
    cpath = [quote(i, safe="") for i in path] if path else ""
    endpoint = "/{}".format(cid) if cid else "/by-path/{}".format("/".join(cpath).replace('"', ""))
    return _get(base_url + "/api/v3/catalog{}".format(endpoint), token, idpath, ssl_verify=ssl_verify)

def sql_endpoint(api_parameters: Parameters, query, context=None, ssl_verify=True):
    url = UrlBuilder.sql_url(api_parameters.base_url, api_parameters.is_cloud, api_parameters.cloud_project_id)
    return _post(url, api_parameters.authentication.get_headers(), ssl_verify=ssl_verify, json={"sql": query, "context": context})

def job_status(api_parameters: Parameters, job_id, ssl_verify=True):
    url = UrlBuilder.job_status_url(api_parameters.base_url, job_id, api_parameters.is_cloud, api_parameters.cloud_project_id)
    return _get(url, api_parameters.authentication.get_headers(), ssl_verify=ssl_verify)

def job_cancel(api_parameters: Parameters, job_id, ssl_verify=True):
    url = UrlBuilder.job_cancel_url(api_parameters.base_url, job_id, api_parameters.is_cloud)
    return _post(url, api_parameters.authentication.get_headers(), json=None, ssl_verify=ssl_verify)

def job_results(api_parameters: Parameters, job_id, offset=0, limit=100, ssl_verify=True):
    url = UrlBuilder.job_results_url(api_parameters.base_url, job_id, api_parameters.is_cloud, offset, limit, api_parameters.cloud_project_id)
    return _get(
        url,
        api_parameters.authentication.get_headers(),
        ssl_verify=ssl_verify,
    )

def delete_catalog(token, base_url, cid, tag, ssl_verify=True):
    """ remove a catalog item from Dremio

    https://docs.dremio.com/rest-api/catalog/delete-catalog-id.html

    :param token: auth token
    :param base_url: sql query
    :param cid: id of a catalog entity
    :param tag: version tag of entity
    :param ssl_verify: ignore ssl errors if False
    :return: None
    """
    # if tag is None:
    #     return _delete(base_url + "/api/v3/catalog/{}".format(cid), token, ssl_verify=ssl_verify)
    # else:
    #     return _delete(base_url + "/api/v3/catalog/{}?tag={}".format(cid, tag), token, ssl_verify=ssl_verify)


def set_catalog(token, base_url, json, ssl_verify=True):
    """ add a new catalog entity

    https://docs.dremio.com/rest-api/catalog/post-catalog.html

    :param token: auth token
    :param base_url: sql query
    :param json: json document for new catalog entity
    :param ssl_verify: ignore ssl errors if False
    :return: new catalog entity
    """
    return _post(base_url + "/api/v3/catalog", token, json, ssl_verify=ssl_verify)


def update_catalog(token, base_url, cid, json, ssl_verify=True):
    """ update a catalog entity

    https://docs.dremio.com/rest-api/catalog/put-catalog-id.html

    :param token: auth token
    :param base_url: sql query
    :param cid: id of catalog entity
    :param json: json document for new catalog entity
    :param ssl_verify: ignore ssl errors if False
    :return: updated catalog entity
    """
    # return _put(base_url + "/api/v3/catalog/{}".format(cid), token, json, ssl_verify=ssl_verify)


def promote_catalog(token, base_url, cid, json, ssl_verify=True):
    """ promote a catalog entity (only works on folders and files in sources

    https://docs.dremio.com/rest-api/catalog/post-catalog-id.html

    :param token: auth token
    :param base_url: sql query
    :param cid: id of catalog entity
    :param json: json document for new catalog entity
    :param ssl_verify: ignore ssl errors if False
    :return: updated catalog entity
    """
    return _post(base_url + "/api/v3/catalog/{}".format(cid), token, json, ssl_verify=ssl_verify)

def collaboration_tags(token, base_url, cid, ssl_verify=True):
    """fetch tags for a catalog entry

    https://docs.dremio.com/rest-api/user/get-catalog-collaboration.html

    :param token: auth token
    :param base_url: sql query
    :param cid: id of a catalog entity
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    """
    return _get(base_url + "/api/v3/catalog/{}/collaboration/tag".format(cid), token, ssl_verify=ssl_verify)

def collaboration_wiki(token, base_url, cid, ssl_verify=True):
    """fetch wiki for a catalog entry

    https://docs.dremio.com/rest-api/user/get-catalog-collaboration.html

    :param token: auth token
    :param base_url: sql query
    :param cid: id of a catalog entity
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    """
    return _get(base_url + "/api/v3/catalog/{}/collaboration/wiki".format(cid), token, ssl_verify=ssl_verify)

def set_collaboration_tags(token, base_url, cid, tags, ssl_verify=True):
    """ set tags on a given catalog entity

    https://docs.dremio.com/rest-api/catalog/post-catalog-collaboration.html

    :param token: auth token
    :param base_url: sql query
    :param cid: id of a catalog entity
    :param tags: list of strings for tags
    :param ssl_verify: ignore ssl errors if False
    :return: None
    """
    json = {"tags": tags}
    try:
        old_tags = collaboration_tags(token, base_url, cid, ssl_verify)
        json["version"] = old_tags["version"]
    except:  # NOQA
        pass
    return _post(base_url + "/api/v3/catalog/{}/collaboration/tag".format(cid), token, ssl_verify=ssl_verify, json=json)

def set_collaboration_wiki(token, base_url, cid, wiki, ssl_verify=True):
    """ set wiki on a given catalog entity

    https://docs.dremio.com/rest-api/catalog/post-catalog-collaboration.html

    :param token: auth token
    :param base_url: sql query
    :param cid: id of a catalog entity
    :param wiki: text representing markdown for entity
    :param ssl_verify: ignore ssl errors if False
    :return: None
    """
    json = {"text": wiki}
    try:
        old_wiki = collaboration_wiki(token, base_url, cid, ssl_verify)
        json["version"] = old_wiki["version"]
    except:  # NOQA
        pass
    return _post(
        base_url + "/api/v3/catalog/{}/collaboration/wiki".format(cid), token, ssl_verify=ssl_verify, json=json
    )

