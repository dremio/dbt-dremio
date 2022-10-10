
class UrlBuilder:
    SOFTWARE_LOGIN_ENDPOINT = '/apiv2/login'

    SOFTWARE_SQL_ENDPOINT = '/api/v3/sql'
    CLOUD_SQL_ENDPOINT = '/v0/sql'

    SOFTWARE_JOB_ENDPOINT = '/api/v3/job/'
    CLOUD_JOB_ENDPOINT = 'v0/job/'

    @classmethod
    def login_url(cls, base_url):
        return base_url + UrlBuilder.SOFTWARE_LOGIN_ENDPOINT
    
    @classmethod
    def sql_url(cls, base_url, is_cloud = False):
        if is_cloud == True:
            return base_url + UrlBuilder.CLOUD_SQL_ENDPOINT
        return base_url + UrlBuilder.SOFTWARE_SQL_ENDPOINT

    @classmethod
    def job_status_url(cls, base_url, job_id, is_cloud = False):
        if is_cloud == True:
            return base_url + UrlBuilder.CLOUD_JOB_ENDPOINT + job_id
        return base_url + UrlBuilder.SOFTWARE_JOB_ENDPOINT + job_id

        #"/api/v3/job/{}".format(job_id)

    @classmethod
    def job_cancel_url(cls, base_url, job_id, is_cloud = False, offset=0, limit=100):
        url_path = None
        if is_cloud == True:
            url_path = base_url + UrlBuilder.CLOUD_JOB_ENDPOINT
        else:
            url_path = base_url + UrlBuilder.SOFTWARE_JOB_ENDPOINT

        return url_path + "{}/cancel".format(job_id)

    @classmethod
    def job_results_url(cls, base_url, job_id, is_cloud = False, offset=0, limit=100):
        url_path = None
        if is_cloud == True:
            url_path = base_url + UrlBuilder.CLOUD_JOB_ENDPOINT
        else:
            url_path = base_url + UrlBuilder.SOFTWARE_JOB_ENDPOINT

        return url_path + "{}/results?offset={}&limit={}".format(job_id, offset, limit)