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

from dbt.adapters.base import Credentials
from dataclasses import dataclass
from typing import Optional
from dbt.adapters.dremio.relation import DremioRelation


@dataclass
class DremioCredentials(Credentials):
    environment: Optional[str]
    database: Optional[str]
    schema: Optional[str]
    datalake: Optional[str]
    root_path: Optional[str]
    cloud_project_id: Optional[str] = None
    cloud_host: Optional[str] = None
    software_host: Optional[str] = None
    UID: Optional[str] = None
    PWD: Optional[str] = None
    port: Optional[int] = 9047  # for rest endpoint
    use_ssl: Optional[bool] = True
    pat: Optional[str] = None
    additional_parameters: Optional[str] = None

    _ALIASES = {
        "user": "UID",
        "username": "UID",
        "pass": "PWD",
        "password": "PWD",
        "server": "host",
        "track": "environment",
        "space": "database",
        "folder": "schema",
        "materialization_database": "datalake",
        "materialization_schema": "root_path",
    }

    @property
    def type(self):
        return "dremio"

    @property
    def unique_field(self):
        return self.host

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'
        # raise NotImplementedError

        return (
            "driver",
            "cloud_host",
            "cloud_project_id",
            "software_host",
            "port",
            "UID",
            "database",
            "schema",
            "additional_parameters",
            "datalake",
            "root_path",
            "environment",
            "use_ssl",
        )

    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(data)
        if "cloud_host" not in data:
            data["cloud_host"] = None
        if "software_host" not in data:
            data["software_host"] = None
        if "database" not in data:
            data["database"] = None
        if "schema" not in data:
            data["schema"] = None
        if "datalake" not in data:
            data["datalake"] = None
        if "root_path" not in data:
            data["root_path"] = None
        if "environment" not in data:
            data["environment"] = None
        if "pat" not in data:
            data["pat"] = None
        return data

    def __post_init__(self):
        if self.database is None:
            self.database = "@" + self.UID
        if self.schema is None:
            self.schema = DremioRelation.no_schema
        if self.datalake is None:
            self.datalake = "$scratch"
        if self.root_path is None:
            self.root_path = DremioRelation.no_schema
