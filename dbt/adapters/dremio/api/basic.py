# -*- coding: utf-8 -*-
#
# Copyright (c) 2019 Ryan Murray.
#
# This file is part of Dremio Client
# (see https://github.com/rymurr/dremio_client).
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import requests

from dbt.adapters.dremio.api.parameters import Parameters
from dbt.adapters.dremio.api.authentication import DremioPatAuthentication
from dbt.adapters.dremio.api.url_builder import UrlBuilder


import json

def login(api_parameters: Parameters, timeout=10, verify=True):

    if isinstance(api_parameters.authentication, DremioPatAuthentication):
        return api_parameters

    url = UrlBuilder.login_url(api_parameters.base_url)

    r = requests.post(url, json={"userName": api_parameters.authentication.username, "password": api_parameters.authentication.password}, timeout=timeout, verify=verify)

    r.raise_for_status()
    
    api_parameters.authentication.token = r.json()["token"]

    return api_parameters
