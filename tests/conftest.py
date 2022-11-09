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

# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]
load_dotenv()


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        "type": "dremio",
        "threads": 1,
        "software_host": os.getenv("DREMIO_HOST"),
        "user": os.getenv("DREMIO_USERNAME"),
        "password": os.getenv("DREMIO_PASSWORD"),
        "datalake": os.getenv("DREMIO_DATALAKE"),
        "use_ssl": False,
    }
