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
    environment: Optional[str] = None
    UID: Optional[str] = None
    PWD: Optional[str] = None
    pat: Optional[str] = None
    object_storage_source: Optional[str] = None
    datalake: Optional[str] = None
    object_storage_path: Optional[str] = None
    root_path: Optional[str] = None
    dremio_space: Optional[str] = None
    database: Optional[str] = None
    dremio_space_folder: Optional[str] = None
    schema: Optional[str] = None
    cloud_project_id: Optional[str] = None
    cloud_host: Optional[str] = None
    software_host: Optional[str] = None
    port: Optional[int] = 9047  # for rest endpoint
    use_ssl: Optional[bool] = True
    additional_parameters: Optional[str] = None

    _ALIASES = {
        # Only terms on right-side will be used going forward.
        "username": "UID",
        "user": "UID",  # backwards compatibility with existing profiles
        "password": "PWD",
        "object_storage_source": "datalake",
        "object_storage_path": "root_path",
        "dremio_space": "database",
        "dremio_space_folder": "schema",
        "folder": "schema",  # backwards compatibility with existing profiles
    }

    _DEFAULT_OBJECT_STORAGE_SOURCE = "$scratch"
    _SPACE_NAME_PLACEHOLDER = "@user"

    @property
    def type(self):
        return "dremio"

    @property
    def unique_field(self):
        return self.host

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'
        return (
            "cloud_host",
            "cloud_project_id",
            "software_host",
            "port",
            "username",
            "UID",
            "additional_parameters",  # TODO: investigate
            "environment",
            "use_ssl",
            "object_storage_source",
            "space_root_folder",
            "root_path",
            "datalake",
            "dremio_space",
            "database",
            "space_root_folder",
            "schema",
            "folder",
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

        if "pat" not in data:
            data["pat"] = None

        if "environment" not in data:
            data["environment"]

        return data

    def __post_init__(self):
        if self.datalake is None:
            self.datalake = self._DEFAULT_OBJECT_STORAGE_SOURCE
        if self.root_path is None:
            self.root_path = DremioRelation.no_schema
        if self.database is None or self.database == self._SPACE_NAME_PLACEHOLDER:
            self.database = f"@{self.UID}"
        if self.schema is None:
            self.schema = DremioRelation.no_schema
