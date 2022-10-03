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
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


class DremioException(Exception):
    """
    base dremio exception
    """
"""
{
  "errorMessage": "brief error message",
  "moreInfo": "detailed error message"
}
"""

    def __init__(self, msg, original_exception, response=None):
        super(DremioException, self).__init__(msg + (": %s" % original_exception))
        self.original_exception = original_exception
        self.response = response


class DremioUnauthorizedException(DremioException):
    pass

class DremioPermissionException(DremioException):
    pass

class DremioNotFoundException(DremioException):
    pass

class DremioBadRequestException(DremioException):
    pass

class DremioAlreadyExistsException(DremioException):
    pass
