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
