import agate

from dbt.adapters.dremio.api.endpoints import sql_endpoint, job_status, job_results, job_cancel, create_catalog
from dbt.adapters.dremio.api.parameters import Parameters

from dbt.events import AdapterLogger
logger = AdapterLogger("dremio")

class DremioCursor:
    def __init__(self, api_parameters: Parameters):
        self._parameters = api_parameters
        
        self._closed = False
        self._job_id = None
        self._rowcount = -1
        self._job_results = None
        self._table_results: agate.Table = None
    
    @property
    def parameters(self):
        return self._parameters

    @property
    def closed(self):
        return self._closed
    
    @closed.setter
    def closed(self, new_closed_value):
        self._closed = new_closed_value
    
    @property
    def rowcount(self):
        return self._rowcount
    
    @property
    def table(self) -> agate.Table:
        return self._table_results

    def job_results(self):
        if self.closed:
            raise Exception("CursorClosed")
        if self._job_results == None:
            self._populate_job_results()

        return self._job_results
    
    def job_cancel(self):
        #cancels current job
        logger.debug(f"Cancelling job {self._job_id}")
        return job_cancel(self._parameters, self._job_id)

    def close(self):
        if self.closed:
            raise Exception("CursorClosed")
        self._initialize()
        self.closed = True

    def execute(self, sql, bindings=None):
        if self.closed:
            raise Exception("CursorClosed")
        if bindings is None:
            self._initialize()

            json_payload = sql_endpoint(self._parameters, sql, context=None, ssl_verify=True)
            
            ## if "id" in json_payload:
            self._job_id = json_payload["id"]
            self._populate_rowcount()
            self._populate_job_results()
            self._populate_results_table()

        else:
            raise Exception("Bindings not currently supported.")

    def fetchone(self):
        row = None
        if self._table_results != None:
            row = self._table_results.rows.get(0)
        
        return row

    def _initialize(self):
        self._job_id = None
        self._rowcount = -1
        self._table_results = None
        self._job_results = None

    def _populate_rowcount(self):
        if self.closed:
            raise Exception("CursorClosed")
        ## keep checking job status until status is one of COMPLETE, CANCELLED or FAILED
        ## map job results to AdapterResponse
        job_id = self._job_id

        last_job_state = ""
        job_status_response = job_status(self._parameters, job_id, ssl_verify=True)
        job_status_state = job_status_response["jobState"]

        while True:
            if job_status_state != last_job_state:
                logger.debug(f"Job State = {job_status_state}")
            if job_status_state == "COMPLETED" or job_status_state == "CANCELLED" or job_status_state == "FAILED":
                break
            last_job_state = job_status_state
            job_status_response = job_status(self._parameters, job_id, ssl_verify=True)
            job_status_state = job_status_response["jobState"]

        #this is done as job status does not return a rowCount if there are no rows affected (even in completed job_state)
        #pyodbc Cursor documentation states "[rowCount] is -1 if no SQL has been executed or if the number of rows is unknown.
        # Note that it is not uncommon for databases to report -1 immediately after a SQL select statement for performance reasons."
        if "rowCount" not in job_status_response:
            rows = -1
            logger.debug("rowCount does not exist in job_status payload")
        else:
            rows = job_status_response["rowCount"]
        
        self._rowcount = rows
    
    def _populate_job_results(self):
        if self._job_results == None:
            self._job_results = job_results(self._parameters, self._job_id, offset=0, limit=100, ssl_verify=True)

    def _populate_results_table(self):
        if self._job_results != None:
            self._table_results = agate.Table.from_object(self._job_results)


