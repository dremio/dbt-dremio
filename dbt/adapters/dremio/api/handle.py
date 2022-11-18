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

from dbt.adapters.dremio.api.cursor import DremioCursor
from dbt.adapters.dremio.api.parameters import Parameters
from dbt.adapters.dremio.api.rest.endpoints import login

from dbt.events import AdapterLogger

logger = AdapterLogger("dremio")


class DremioHandle:
    def __init__(self, parameters: Parameters):
        self._parameters = parameters
        self._cursor = None
        self.closed = False

    def get_parameters(self):
        return self._parameters

    def cursor(self):
        if self.closed:
            raise Exception("HandleClosed")
        if self._cursor is None or self._cursor.closed:
            self._parameters = login(self._parameters)
            self._cursor = DremioCursor(self._parameters)
        return self._cursor

    def close(self):
        if self.closed:
            raise Exception("HandleClosed")
        self.closed = True

    def rollback(self):
        # todo
        logger.debug("Handle rollback not implemented.")
