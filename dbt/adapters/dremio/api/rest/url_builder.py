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

from dbt.adapters.dremio.api.parameters import (
    Parameters,
    CloudParameters,
    SoftwareParameters,
)
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

    DREMIO_WIKIS_ENDPOINT = "/collaboration/wiki"

    DREMIO_TAGS_ENDPOINT = "/collaboration/tag"

    SOFTWARE_REFLECTIONS_ENDPOINT = "/api/v3/reflection"
    CLOUD_REFLECTIONS_ENDPOINT = CLOUD_PROJECT_ENDPOINT + "/{}/reflection"

    SOFTWARE_DATASET_ENDPOIT = "/api/v3/dataset"
    CLOUD_DATASET_ENDPOINT = CLOUD_PROJECT_ENDPOINT + "/{}/dataset"

    # https://docs.dremio.com/software/rest-api/jobs/get-job/
    OFFSET_DEFAULT = 0
    LIMIT_DEFAULT = 100

    # login_url only takes SoftwareParameters because Cloud uses pat.
    # There is no need to login to retrieve a token when Cloud only uses pat.
    @classmethod
    def login_url(cls, software_parameters: SoftwareParameters):
        return software_parameters.base_url + UrlBuilder.SOFTWARE_LOGIN_ENDPOINT

    @classmethod
    def sql_url(cls, parameters: Parameters):
        if type(parameters) is CloudParameters:
            return parameters.base_url + UrlBuilder.CLOUD_SQL_ENDPOINT.format(
                parameters.cloud_project_id
            )
        return parameters.base_url + UrlBuilder.SOFTWARE_SQL_ENDPOINT

    @classmethod
    def job_status_url(cls, parameters: Parameters, job_id):
        if type(parameters) is CloudParameters:
            return (
                    parameters.base_url
                    + UrlBuilder.CLOUD_JOB_ENDPOINT.format(parameters.cloud_project_id)
                    + "/"
                    + job_id
            )
        return parameters.base_url + UrlBuilder.SOFTWARE_JOB_ENDPOINT + "/" + job_id

    @classmethod
    def job_cancel_url(cls, parameters: Parameters, job_id):
        url_path = parameters.base_url
        if type(parameters) is CloudParameters:
            url_path += UrlBuilder.CLOUD_JOB_ENDPOINT
        else:
            url_path += UrlBuilder.SOFTWARE_JOB_ENDPOINT

        return url_path + "/{}/cancel".format(job_id)

    @classmethod
    def job_results_url(
            cls,
            parameters: Parameters,
            job_id,
            offset=OFFSET_DEFAULT,
            limit=LIMIT_DEFAULT,
    ):
        url_path = parameters.base_url
        if type(parameters) is CloudParameters:
            url_path += UrlBuilder.CLOUD_JOB_ENDPOINT.format(
                parameters.cloud_project_id
            )
        else:
            url_path += UrlBuilder.SOFTWARE_JOB_ENDPOINT

        return url_path + "/{}/results?offset={}&limit={}".format(job_id, offset, limit)

    @classmethod
    def catalog_url(cls, parameters: Parameters):
        url_path = parameters.base_url
        if type(parameters) is CloudParameters:
            url_path += UrlBuilder.CLOUD_CATALOG_ENDPOINT.format(
                parameters.cloud_project_id
            )
        else:
            url_path += UrlBuilder.SOFTWARE_CATALOG_ENDPOINT

        return url_path

    @classmethod
    def delete_catalog_url(cls, parameters: Parameters, cid):
        url_path = cls.catalog_url(parameters=parameters)
        url_path += f"/{cid}"

        return url_path

    @classmethod
    def catalog_item_by_id_url(cls, parameters: Parameters, catalog_id):
        url_path = parameters.base_url
        if type(parameters) is CloudParameters:
            url_path += UrlBuilder.CLOUD_CATALOG_ENDPOINT.format(
                parameters.cloud_project_id
            )
        else:
            url_path += UrlBuilder.SOFTWARE_CATALOG_ENDPOINT

        endpoint = "/{}".format(catalog_id)
        return url_path + endpoint

    @classmethod
    def catalog_item_by_path_url(cls, parameters: Parameters, path_list):
        url_path = parameters.base_url
        if type(parameters) is CloudParameters:
            url_path += UrlBuilder.CLOUD_CATALOG_ENDPOINT.format(
                parameters.cloud_project_id
            )
        else:
            url_path += UrlBuilder.SOFTWARE_CATALOG_ENDPOINT

        # Escapes special characters
        quoted_path_list = [quote(i, safe="") for i in path_list]
        # Converts list to string separated by '/'
        joined_path_str = "/".join(quoted_path_list).replace('"', "")
        endpoint = f"/by-path/{joined_path_str}"
        return url_path + endpoint
    
    # dbt docs integration within Dremio wikis and tags
    @classmethod
    def wikis_management_url(cls, parameters: Parameters, object_id: str) -> str:
        return cls.catalog_url(parameters) + f"/{object_id}{UrlBuilder.DREMIO_WIKIS_ENDPOINT}"
    
    @classmethod
    def tags_management_url(cls, parameters: Parameters, dataset_id: str) -> str:
        return cls.catalog_url(parameters) + f"/{dataset_id}{UrlBuilder.DREMIO_TAGS_ENDPOINT}"

    @classmethod
    def create_reflection_url(cls, parameters: Parameters):
        url_path = parameters.base_url
        if type(parameters) is CloudParameters:
            url_path += UrlBuilder.CLOUD_REFLECTIONS_ENDPOINT.format(
                parameters.cloud_project_id
            )
        else:
            url_path += UrlBuilder.SOFTWARE_REFLECTIONS_ENDPOINT

        return url_path

    @classmethod
    def update_reflection_url(cls, parameters: Parameters, dataset_id):
        url_path = parameters.base_url
        if type(parameters) is CloudParameters:
            url_path += UrlBuilder.CLOUD_REFLECTIONS_ENDPOINT.format(
                parameters.cloud_project_id
            )
        else:
            url_path += UrlBuilder.SOFTWARE_REFLECTIONS_ENDPOINT

        endpoint = "/{}".format(dataset_id)
        return url_path + endpoint

    @classmethod
    def get_reflection_url(cls, parameters: Parameters, dataset_id):
        url_path = parameters.base_url
        if type(parameters) is CloudParameters:
            url_path += UrlBuilder.CLOUD_DATASET_ENDPOINT.format(
                parameters.cloud_project_id
            )
        else:
            url_path += UrlBuilder.SOFTWARE_DATASET_ENDPOIT

        endpoint = "/{}/reflection".format(dataset_id)
        return url_path + endpoint
