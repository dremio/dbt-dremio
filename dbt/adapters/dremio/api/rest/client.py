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


import requests

from dbt.adapters.dremio.api.authentication import DremioPatAuthentication
from dbt.adapters.dremio.api.parameters import Parameters
from dbt.adapters.dremio.api.rest.utils import _post, _get, _put, _delete
from dbt.adapters.dremio.api.rest.url_builder import UrlBuilder

from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("dremio")

session = requests.Session()


class DremioRestClient:
    def __init__(self, api_parameters: Parameters):
        self._parameters = api_parameters

    def start(self):
        self._parameters = self.__login()

    def __login(self, timeout=10):
        if isinstance(self._parameters.authentication, DremioPatAuthentication):
            return self._parameters

        url = UrlBuilder.login_url(self._parameters)
        response = _post(
            url,
            json={
                "userName": self._parameters.authentication.username,
                "password": self._parameters.authentication.password,
            },
            timeout=timeout,
            ssl_verify=self._parameters.authentication.verify_ssl,
        )

        self._parameters.authentication.token = response["token"]

        return self._parameters

    def sql_endpoint(self, query, context=None):
        url = UrlBuilder.sql_url(self._parameters)
        return _post(
            url,
            self._parameters.authentication.get_headers(),
            ssl_verify=self._parameters.authentication.verify_ssl,
            json={"sql": query, "context": context},
        )

    def job_status(self, job_id):
        url = UrlBuilder.job_status_url(self._parameters, job_id)
        return _get(
            url,
            self._parameters.authentication.get_headers(),
            ssl_verify=self._parameters.authentication.verify_ssl,
        )

    def job_cancel_api(self, job_id):
        url = UrlBuilder.job_cancel_url(self._parameters, job_id)
        return _post(
            url,
            self._parameters.authentication.get_headers(),
            json=None,
            ssl_verify=self._parameters.authentication.verify_ssl,
        )

    def job_results(self, job_id, offset=0, limit=100):
        url = UrlBuilder.job_results_url(
            self._parameters,
            job_id,
            offset,
            limit,
        )
        return _get(
            url,
            self._parameters.authentication.get_headers(),
            ssl_verify=self._parameters.authentication.verify_ssl,
        )

    def create_catalog_api(self, json):
        url = UrlBuilder.catalog_url(self._parameters)
        return _post(
            url,
            self._parameters.authentication.get_headers(),
            json=json,
            ssl_verify=self._parameters.authentication.verify_ssl,
        )

    def get_catalog_item(self, catalog_id=None, catalog_path=None):
        if catalog_id is None and catalog_path is None:
            raise TypeError(
                "both id and path can't be None for a catalog_item call")

        # Will use path if both id and path are specified
        if catalog_path:
            url = UrlBuilder.catalog_item_by_path_url(
                self._parameters,
                catalog_path,
            )
        else:
            url = UrlBuilder.catalog_item_by_id_url(
                self._parameters,
                catalog_id,
            )
        return _get(
            url,
            self._parameters.authentication.get_headers(),
            ssl_verify=self._parameters.authentication.verify_ssl,
        )

    def delete_catalog(self, cid):
        url = UrlBuilder.delete_catalog_url(self._parameters, cid)
        return _delete(
            url,
            self._parameters.authentication.get_headers(),
            ssl_verify=self._parameters.authentication.verify_ssl,
        )
    
    # dbt docs integration within Dremio wikis and tags
    def create_wiki(self, object_id: str, text: str):
        url = UrlBuilder.wikis_management_url(self._parameters, object_id)
        return _post(
            url,
            self._parameters.authentication.get_headers(),
            json={"text": text},
            ssl_verify=self._parameters.authentication.verify_ssl,
        )

    def retrieve_wiki(self, object_id: str):
        url = UrlBuilder.wikis_management_url(self._parameters, object_id)
        return _get(
            url,
            self._parameters.authentication.get_headers(),
            ssl_verify=self._parameters.authentication.verify_ssl,
        )

    def update_wiki(self, object_id: str, text: str, version: int):
        url = UrlBuilder.wikis_management_url(self._parameters, object_id)
        return _post(
            url,
            self._parameters.authentication.get_headers(),
            json={"text": text, "version": version},
            ssl_verify=self._parameters.authentication.verify_ssl,
        )

    def delete_wiki(self, object_id: str, version: int):
        url = UrlBuilder.wikis_management_url(self._parameters, object_id)
        return _post(
            url,
            self._parameters.authentication.get_headers(),
            json={"text": "", "version": version},
            ssl_verify=self._parameters.authentication.verify_ssl,
        )


    def create_tags(self, dataset_id: str, tags: list[str]):
        url = UrlBuilder.tags_management_url(self._parameters, dataset_id)
        return _post(
            url,
            self._parameters.authentication.get_headers(),
            json={"tags": tags},
            ssl_verify=self._parameters.authentication.verify_ssl,
        )

    def retrieve_tags(self, dataset_id: str):
        url = UrlBuilder.tags_management_url(self._parameters, dataset_id)
        return _get(
            url,
            self._parameters.authentication.get_headers(),
            ssl_verify=self._parameters.authentication.verify_ssl,
        )

    def update_tags(self, dataset_id: str, tags: list[str], version: str):
        url = UrlBuilder.tags_management_url(self._parameters, dataset_id)
        return _post(
            url,
            self._parameters.authentication.get_headers(),
            json={"tags": tags, "version": version},
            ssl_verify=self._parameters.authentication.verify_ssl,
        )

    def delete_tags(self, dataset_id: str, version: str):
        url = UrlBuilder.tags_management_url(self._parameters, dataset_id)
        return _post(
            url,
            self._parameters.authentication.get_headers(),
            json={"tags": [], "version": version},
            ssl_verify=self._parameters.authentication.verify_ssl,
        )


    def get_reflections(self, dataset_id):
        url = UrlBuilder.get_reflection_url(self._parameters, dataset_id)
        return _get(
            url,
            self._parameters.authentication.get_headers(),
            ssl_verify=self._parameters.authentication.verify_ssl,
        )

    def create_reflection(self, payload):
        url = UrlBuilder.create_reflection_url(self._parameters)
        return _post(
            url,
            self._parameters.authentication.get_headers(),
            json=payload,
            ssl_verify=self._parameters.authentication.verify_ssl,
        )

    def update_reflection(self, reflection_id, payload):
        url = UrlBuilder.update_reflection_url(self._parameters, reflection_id)
        return _put(
            url,
            self._parameters.authentication.get_headers(),
            json=payload,
            ssl_verify=self._parameters.authentication.verify_ssl,
        )
    