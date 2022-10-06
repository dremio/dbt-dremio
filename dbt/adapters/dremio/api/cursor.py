from dbt.adapters.dremio.api.endpoints import sql_endpoint, job_status, job_results
from dbt import exceptions as dbtexceptions

#from dbt.logger import GLOBAL_LOGGER as logger
from dbt.events import AdapterLogger
logger = AdapterLogger("dremio")

class DremioCursor:
    def __init__(self, host, port, token):
        self._host = host
        self._port = port
        self._token = token
        self._job_id = None
        self._closed = False

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
    def closed(self):
        return self._closed

    @token.setter
    def token(self, new_token):
        self._token = new_token
    
    @job_id.setter
    def job_id(self, new_job_id):
        self._job_id = new_job_id
    
    @closed.setter
    def closed(self, new_closed_value):
        self._closed = new_closed_value

    def __build_base_url(self):
        return "http://{host}:{port}".format(host=self._host, port=self._port)

    def job_results(self):
        if self.closed:
            raise Exception("CursorClosed")
        if not self.closed:
            base_url = self.__build_base_url()
            json_payload = job_results(self._token, base_url, self._job_id, offset=0, limit=100, ssl_verify=True)

        return json_payload

    def close(self):
        if self.closed:
            raise Exception("CursorClosed")
        self.closed = True

    def execute(self, sql, bindings=None):
        if self.closed:
            raise Exception("CursorClosed")
        if bindings is None:
            base_url = self.__build_base_url()
            json_payload = sql_endpoint(self._token, base_url, sql, context=None, ssl_verify=True)
            self._job_id = json_payload["id"]
        else:
            raise Exception("Bindings not currently supported.")

    @property
    def rowcount(self):
        if self.closed:
            raise Exception("CursorClosed")
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
            if job_status_state == "COMPLETED" or job_status_state == "CANCELLED" or job_status_state == "FAILED":
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
    

