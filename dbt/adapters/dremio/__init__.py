from dbt.adapters.dremio.connections import DremioConnectionManager
from dbt.adapters.dremio.credentials import DremioCredentials
from dbt.adapters.dremio.impl import DremioAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import dremio


Plugin = AdapterPlugin(
    adapter=DremioAdapter,
    credentials=DremioCredentials,
    include_path=dremio.PACKAGE_PATH,
)
