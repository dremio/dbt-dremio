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

import os
import pytest
from dotenv import load_dotenv

# Import the functional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]
load_dotenv()


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="dremio_cloud", type=str)


@pytest.fixture(scope="session")
def dbt_profile_target(request):
    profile_type = request.config.getoption("--profile")
    if profile_type == "dremio_software":
        target = dremio_software_target()
    elif profile_type == "dremio_cloud":
        target = dremio_cloud_target()
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")
    return target


def dremio_cloud_target():
    return {
        "type": "dremio",
        "threads": 1,
        "cloud_host": os.getenv("DREMIO_CLOUD_HOST"),
        "cloud_project_id": os.getenv("DREMIO_CLOUD_PROJECT_ID"),
        "user": os.getenv("DREMIO_CLOUD_USERNAME"),
        "pat": os.getenv("DREMIO_PAT"),
        "datalake": os.getenv("DREMIO_DATALAKE"),
        "use_ssl": True,
        # Need to include a specific space for grants tests
        "database": os.getenv("DREMIO_DATABASE"),
    }


def dremio_software_target():
    return {
        "type": "dremio",
        "threads": 1,
        "software_host": os.getenv("DREMIO_SOFTWARE_HOST"),
        "user": os.getenv("DREMIO_SOFTWARE_USERNAME"),
        "password": os.getenv("DREMIO_SOFTWARE_PASSWORD"),
        "datalake": os.getenv("DREMIO_DATALAKE"),
        "use_ssl": False,
        # Need to include a specific space for grants tests
        "database": os.getenv("DREMIO_DATABASE"),
    }
