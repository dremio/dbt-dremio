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

from dbt.adapters.dremio.api.error import DremioException
from urllib.parse import quote


class UrlBuilder:
    SOFTWARE_LOGIN_ENDPOINT = "/apiv2/login"

    CLOUD_PROJECT_ENDPOINT = "/v0/projects"

    SOFTWARE_SQL_ENDPOINT = "/api/v3/sql"
    CLOUD_SQL_ENDPOINT = CLOUD_PROJECT_ENDPOINT + "/{}/sql"

    SOFTWARE_JOB_ENDPOINT = "/api/v3/job"
    CLOUD_JOB_ENDPOINT = CLOUD_PROJECT_ENDPOINT + "/{}/job"

    SOFTWARE_CATALOG_ENDPOINT = "/api/v3/catalog"
    CLOUD_CATALOG_ENDPOINT = CLOUD_PROJECT_ENDPOINT + "/{}/catalog"

    @classmethod
    def login_url(cls, base_url):
        return base_url + UrlBuilder.SOFTWARE_LOGIN_ENDPOINT

    @classmethod
    def sql_url(cls, base_url, is_cloud=False, cloud_project_id=None):
        if is_cloud:
            return base_url + UrlBuilder.CLOUD_SQL_ENDPOINT.format(cloud_project_id)
        return base_url + UrlBuilder.SOFTWARE_SQL_ENDPOINT

    @classmethod
    def job_status_url(cls, base_url, job_id, is_cloud=False, cloud_project_id=None):
        if is_cloud:
            return (
                base_url
                + UrlBuilder.CLOUD_JOB_ENDPOINT.format(cloud_project_id)
                + "/"
                + job_id
            )
        return base_url + UrlBuilder.SOFTWARE_JOB_ENDPOINT + "/" + job_id

    @classmethod
    def job_cancel_url(cls, base_url, job_id, is_cloud=False, offset=0, limit=100):
        url_path = None
        if is_cloud:
            url_path = base_url + UrlBuilder.CLOUD_JOB_ENDPOINT
        else:
            url_path = base_url + UrlBuilder.SOFTWARE_JOB_ENDPOINT

        return url_path + "/{}/cancel".format(job_id)

    @classmethod
    def job_results_url(
        cls,
        base_url,
        job_id,
        is_cloud=False,
        offset=0,
        limit=100,
        cloud_project_id=None,
    ):
        url_path = None
        if is_cloud:
            url_path = base_url + UrlBuilder.CLOUD_JOB_ENDPOINT.format(cloud_project_id)
        else:
            url_path = base_url + UrlBuilder.SOFTWARE_JOB_ENDPOINT

        return url_path + "/{}/results?offset={}&limit={}".format(job_id, offset, limit)

    @classmethod
    def catalog_url(cls, base_url, is_cloud=False, cloud_project_id=None):
        url_path = None
        if is_cloud:
            url_path = base_url + UrlBuilder.CLOUD_CATALOG_ENDPOINT.format(
                cloud_project_id
            )
        else:
            url_path = base_url + UrlBuilder.SOFTWARE_CATALOG_ENDPOINT

        return url_path

    @classmethod
    def catalog_item_by_id_url(
        cls, base_url, catalog_id, is_cloud=False, cloud_project_id=None
    ):
        url_path = None
        if is_cloud:
            url_path = base_url + UrlBuilder.CLOUD_CATALOG_ENDPOINT.format(
                cloud_project_id
            )
        else:
            url_path = base_url + UrlBuilder.SOFTWARE_CATALOG_ENDPOINT

        endpoint = "/{}".format(catalog_id)
        return url_path + endpoint

    @classmethod
    def catalog_item_by_path_url(
        cls, base_url, path_list, is_cloud=False, cloud_project_id=None
    ):
        url_path = None
        if is_cloud:
            url_path = base_url + UrlBuilder.CLOUD_CATALOG_ENDPOINT.format(
                cloud_project_id
            )
        else:
            url_path = base_url + UrlBuilder.SOFTWARE_CATALOG_ENDPOINT

        # Escapes special characters
        quoted_path_list = [quote(i, safe="") for i in path_list]
        # Converts list to string separated by '/'
        joined_path_str = "/".join(quoted_path_list).replace('"', "")
        endpoint = f"/by-path/{joined_path_str}"
        return url_path + endpoint
