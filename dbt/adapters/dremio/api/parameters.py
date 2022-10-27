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
    def __buildDremioAuthentication(self, credentials: DremioCredentials):
        return DremioAuthentication.build(
            credentials.UID, credentials.PWD, credentials.pat
        )

    @classmethod
    def build(cls, credentials: DremioCredentials):
        if credentials.cloud_host != None and credentials.cloud_project_id != None:
            return CloudParametersBuilder(
                cls.__buildDremioAuthentication(credentials=credentials),
                credentials.cloud_host,
                credentials.cloud_project_id,
            )
        elif (
            credentials.software_host != None
            and credentials.port != None
            and credentials.use_ssl != None
        ):
            return SoftwareParametersBuilder(
                cls.__buildDremioAuthentication(credentials=credentials),
                credentials.software_host,
                credentials.port,
                credentials.use_ssl,
            )
        raise ValueError("Credentials match neither Cloud nor Software")

    @abstractmethod
    def buildBaseUrl(self):
        pass

    @abstractmethod
    def getParameters(self) -> Parameters:
        pass


@dataclass
class CloudParametersBuilder(ParametersBuilder):
    cloud_host: str = None
    cloud_project_id: str = None

    def buildBaseUrl(self):
        protocol = "https"
        base_url = f"{protocol}://{self.cloud_host}"
        return base_url

    def getParameters(self) -> Parameters:
        return CloudParameters(
            base_url=self.buildBaseUrl(),
            authentication=self.authentication,
            cloud_project_id=self.cloud_project_id,
        )


@dataclass
class SoftwareParametersBuilder(ParametersBuilder):
    software_host: str = None
    port: str = None
    use_ssl: bool = None

    def buildBaseUrl(self):
        protocol = "http"
        if self.use_ssl:
            protocol = "https"
        base_url = f"{protocol}://{self.software_host}:{self.port}"
        return base_url

    def getParameters(self) -> Parameters:
        return SoftwareParameters(
            base_url=self.buildBaseUrl(), authentication=self.authentication
        )
