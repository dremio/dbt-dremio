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


from dataclasses import dataclass
from typing import Optional
from abc import abstractmethod

from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("dremio")


@dataclass
class DremioAuthentication:
    username: Optional[str] = None
    verify_ssl: bool = True

    @classmethod
    def build(cls, username: None, password: None, pat: None, verify_ssl: True):
        if password is not None:
            return DremioPasswordAuthentication(
                username=username, verify_ssl=verify_ssl, password=password, token=None
            )
        return DremioPatAuthentication(
            username=username, verify_ssl=verify_ssl, pat=pat
        )

    @classmethod
    def build_headers(cls, authorization_field):
        headers = {
            "Content-Type": "application/json",
            "Authorization": "{authorization_token}".format(
                authorization_token=authorization_field
            ),
        }
        return headers

    @abstractmethod
    def get_headers(self):
        pass


@dataclass
class DremioPasswordAuthentication(DremioAuthentication):
    password: Optional[str] = None
    token: Optional[str] = None

    def get_headers(self):
        authorization_field = "_dremio{authorization_token}".format(
            authorization_token=self.token
        )
        return self.build_headers(authorization_field)


@dataclass
class DremioPatAuthentication(DremioAuthentication):
    pat: Optional[str] = None

    def get_headers(self):
        authorization_field = "Bearer {authorization_token}".format(
            authorization_token=self.pat
        )
        return self.build_headers(authorization_field)
