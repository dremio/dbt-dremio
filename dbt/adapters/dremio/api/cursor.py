from dbt.adapters.dremio.api.basic import login
from dbt.adapters.dremio.api.endpoints import sql_endpoint, job_status, job_results

#from dbt.logger import GLOBAL_LOGGER as logger
from dbt.events import AdapterLogger
logger = AdapterLogger("dremio")

class DremioCursor:
    def __init__(self, host, port, token, job_id):
        self._host = host
        self._port = port
        self._token = token
        self._job_id = job_id
        self._description = None # SQLCONNECTIONMANAGER

    @property
    def host(self):
        return self._host
    
    @property
    def port(self):
        return self._port
    
    @property
    def token(self):
        return self._token

    @property
    def job_id(self):
        return self._job_id

    @property
    def description(self):
        # column names?
        return self.description

    @token.setter
    def token(self, new_token):
        self._token = new_token
    
    @job_id.setter
    def job_id(self, new_job_id):
        self._job_id = new_job_id

    def close(self):
        pass

    def execute(self, sql):
        pass

    def execute(self, sql, bindings):
        pass

    def fetchall(self):# SQLCONNECTIONMANAGER
        pass

    @property
    def rowcount(self):
        ## keep checking job status until status is one of COMPLETE, CANCELLED or FAILED
        ## map job results to AdapterResponse
        token = self._token
        job_id = self._job_id

        base_url = self.__build_base_url()
        last_job_state = ""
        job_status_response = job_status(token, base_url, job_id, ssl_verify=True)
        job_status_state = job_status_response["jobState"]

        while True:
            if job_status_state != last_job_state:
                logger.debug(f"Job State = {job_status_state}")
            if job_status_state == "COMPLETED":
                message = job_status_state
                break
            elif job_status_state == "CANCELLED" or job_status_state == "FAILED":
                message = job_status_state + ": " + job_status_response["errorMessage"]
                break
            last_job_state = job_status_state
            job_status_response = job_status(token, base_url, job_id, ssl_verify=True)
            job_status_state = job_status_response["jobState"]

        #this is done as job status does not return a rowCount if there are no rows affected (even in completed job_state)
        #pyodbc Cursor documentation states "[rowCount] is -1 if no SQL has been executed or if the number of rows is unknown.
        # Note that it is not uncommon for databases to report -1 immediately after a SQL select statement for performance reasons."
        if "rowCount" not in job_status_response:
            rows = -1
            logger.debug("rowCount does not exist in job_status payload")
        else:
            rows = job_status_response["rowCount"]
        
        return rows
    
    def __build_base_url(self):
        return "http://{host}:{port}".format(host=self._host, port=self._port)

