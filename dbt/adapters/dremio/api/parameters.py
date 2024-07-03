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

from abc import abstractmethod
from dataclasses import dataclass
from typing import Optional
from dbt.adapters.dremio.api.authentication import DremioAuthentication
from dbt.adapters.dremio.credentials import DremioCredentials


@dataclass
class Parameters:
    base_url: str
    authentication: DremioAuthentication


@dataclass
class CloudParameters(Parameters):
    cloud_project_id: str


@dataclass
class SoftwareParameters(Parameters):
    pass


@dataclass
class ParametersBuilder:
    authentication: Optional[DremioCredentials] = None

    @classmethod
    def build(cls, credentials: DremioCredentials):
        if (
            credentials.cloud_host is not None
            and credentials.software_host is None
            and credentials.cloud_project_id is not None
        ):
            return CloudParametersBuilder(
                authentication=cls._build_dremio_authentication(
                    credentials=credentials
                ),
                cloud_host=credentials.cloud_host,
                cloud_project_id=credentials.cloud_project_id,
            )
        if (
            credentials.software_host is not None
            and credentials.cloud_host is None
            and credentials.port is not None
            and credentials.use_ssl is not None
        ):
            return SoftwareParametersBuilder(
                authentication=cls._build_dremio_authentication(
                    credentials=credentials
                ),
                software_host=credentials.software_host,
                port=credentials.port,
                use_ssl=credentials.use_ssl,
            )
        raise ValueError("Credentials match neither Cloud nor Software")

    @abstractmethod
    def build_base_url(self):
        pass

    @abstractmethod
    def get_parameters(self) -> Parameters:
        pass

    @classmethod
    def _build_dremio_authentication(self, credentials: DremioCredentials):
        return DremioAuthentication.build(
            credentials.UID, credentials.PWD, credentials.pat, credentials.verify_ssl
        )


@dataclass
class CloudParametersBuilder(ParametersBuilder):
    cloud_host: str = None
    cloud_project_id: str = None

    def build_base_url(self):
        protocol = "https"
        base_url = f"{protocol}://{self.cloud_host}"
        return base_url

    def get_parameters(self) -> Parameters:
        return CloudParameters(
            base_url=self.build_base_url(),
            authentication=self.authentication,
            cloud_project_id=self.cloud_project_id,
        )


@dataclass
class SoftwareParametersBuilder(ParametersBuilder):
    software_host: str = None
    port: str = None
    use_ssl: bool = None

    def build_base_url(self):
        protocol = "http"
        if self.use_ssl:
            protocol = "https"
        base_url = f"{protocol}://{self.software_host}:{self.port}"
        return base_url

    def get_parameters(self) -> Parameters:
        return SoftwareParameters(
            base_url=self.build_base_url(),
            authentication=self.authentication,
        )
