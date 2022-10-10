from xmlrpc.client import boolean
from dataclasses import dataclass
from typing import Optional

from dbt.adapters.dremio.api.authentication import DremioAuthentication

from dbt.events import AdapterLogger
logger = AdapterLogger("dremio")

@dataclass
class Parameters:
    base_url: str
    authentication: DremioAuthentication    
    is_cloud: boolean = True
    cloud_project_id: Optional[str] = None
