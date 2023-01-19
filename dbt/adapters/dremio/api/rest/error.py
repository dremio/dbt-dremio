# Copyright (C) 2022 Dremio Corporation

# Copyright (c) 2019 Ryan Murray.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class DremioException(Exception):
    def __init__(self, msg, original_exception, response=None):
        message = f"{msg}: ({original_exception})"
        if response is not None:
            message += f": ({response.text})"

        self.message = message
        self.original_exception = original_exception
        self.response = response

        super(DremioException, self).__init__(self.message)


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


class DremioRequestTimeoutException(DremioException):
    pass


class DremioTooManyRequestsException(DremioException):
    pass


class DremioInternalServerException(DremioException):
    pass


class DremioServiceUnavailableException(DremioException):
    pass


class DremioGatewayTimeoutException(DremioException):
    pass
