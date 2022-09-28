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


def login(base_url, username, password, timeout=10, verify=True):
    """
    Log into dremio using basic auth
    :param base_url: Dremio url
    :param username: username
    :param password: password
    :param timeout: optional timeout
    :param verify: If false ignore ssl errors
    :return: auth token
    """
    url = base_url + "/apiv2/login"

    r = requests.post(url, json={"userName": username, "password": password}, timeout=timeout, verify=verify)
    r.raise_for_status()
    return r.json()["token"]
