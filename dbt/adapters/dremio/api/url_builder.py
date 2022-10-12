from dbt.adapters.dremio.api.error import DremioException
from urllib.parse import quote

class UrlBuilder:
    SOFTWARE_LOGIN_ENDPOINT = '/apiv2/login'

    CLOUD_PROJECTS_ENDPOINT = '/v0/projects'
    
    SOFTWARE_SQL_ENDPOINT = '/api/v3/sql'
    CLOUD_SQL_ENDPOINT = CLOUD_PROJECTS_ENDPOINT + '/{}/sql'

    SOFTWARE_JOB_ENDPOINT = '/api/v3/job'
    CLOUD_JOB_ENDPOINT = CLOUD_PROJECTS_ENDPOINT + '/{}/job'

    SOFTWARE_CATALOG_ENDPOINT = '/api/v3/catalog'
    CLOUD_CATALOG_ENDPOINT = CLOUD_PROJECTS_ENDPOINT + '/{}/catalog'
    

    @classmethod
    def login_url(cls, base_url):
        return base_url + UrlBuilder.SOFTWARE_LOGIN_ENDPOINT
    
    @classmethod
    def sql_url(cls, base_url, is_cloud = False, cloud_project_id = None):
        if is_cloud == True:
            return base_url + UrlBuilder.CLOUD_SQL_ENDPOINT.format(cloud_project_id)
        return base_url + UrlBuilder.SOFTWARE_SQL_ENDPOINT

    @classmethod
    def job_status_url(cls, base_url, job_id, is_cloud = False, cloud_project_id = None):
        if is_cloud == True:
            return base_url + UrlBuilder.CLOUD_JOB_ENDPOINT.format(cloud_project_id) + '/' + job_id
        return base_url + UrlBuilder.SOFTWARE_JOB_ENDPOINT + '/' + job_id

    @classmethod
    def job_cancel_url(cls, base_url, job_id, is_cloud = False, offset=0, limit=100):
        url_path = None
        if is_cloud == True:
            url_path = base_url + UrlBuilder.CLOUD_JOB_ENDPOINT
        else:
            url_path = base_url + UrlBuilder.SOFTWARE_JOB_ENDPOINT

        return url_path + "/{}/cancel".format(job_id)

    @classmethod
    def job_results_url(cls, base_url, job_id, is_cloud = False, offset=0, limit=100, cloud_project_id = None):
        url_path = None
        if is_cloud == True:
            url_path = base_url + UrlBuilder.CLOUD_JOB_ENDPOINT.format(cloud_project_id)
        else:
            url_path = base_url + UrlBuilder.SOFTWARE_JOB_ENDPOINT

        return url_path + "/{}/results?offset={}&limit={}".format(job_id, offset, limit)

    @classmethod
    def catalog_url(cls, base_url, is_cloud = False, cloud_project_id = None):
        url_path = None
        if is_cloud == True:
            url_path = base_url + UrlBuilder.CLOUD_CATALOG_ENDPOINT.format(cloud_project_id)
        else:
            url_path = base_url + UrlBuilder.SOFTWARE_CATALOG_ENDPOINT

        return url_path

    @classmethod
    def catalog_item_url(cls, base_url, cid, path, is_cloud = False, cloud_project_id = None):
        url_path = None
        if is_cloud == True:
            url_path = base_url + UrlBuilder.CLOUD_CATALOG_ENDPOINT.format(cloud_project_id)
        else:
            url_path = base_url + UrlBuilder.SOFTWARE_CATALOG_ENDPOINT
        if cid is None and path is None:
            raise TypeError("both id and path can't be None for a catalog_item call")
        cpath = [quote(i, safe="") for i in path] if path else ""
        endpoint = "/{}".format(cid) if cid else "/by-path/{}".format("/".join(cpath).replace('"', ""))
        return url_path + endpoint