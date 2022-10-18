# Copyright (C) 2022 Dremio Corporation 
#
# Copyright (c) 2019 Ryan Murray.
#
# Licensed under the Apache License, Version 2.0 (the "License"); 
# you may not use this file except in compliance with the License. 
# You may obtain a copy of the License at 

# http://www.apache.org/licenses/LICENSE-2.0 

# Unless required by applicable law or agreed to in writing, software 
# distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
# See the License for the specific language governing permissions and 
# limitations under the License.

import requests

from dbt.adapters.dremio.api.parameters import Parameters
from dbt.adapters.dremio.api.authentication import DremioPatAuthentication
from dbt.adapters.dremio.api.url_builder import UrlBuilder
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def login(api_parameters: Parameters, timeout=10, verify=True):

    if isinstance(api_parameters.authentication, DremioPatAuthentication):
        return api_parameters

    url = UrlBuilder.login_url(api_parameters.base_url)

    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    r = session.post(url, json={"userName": api_parameters.authentication.username, "password": api_parameters.authentication.password}, timeout=timeout, verify=verify)
    r.raise_for_status()
    
    api_parameters.authentication.token = r.json()["token"]

    return api_parameters
