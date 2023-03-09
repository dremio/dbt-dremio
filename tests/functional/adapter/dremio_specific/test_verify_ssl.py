# Copyright (C) 2022 Dremio Corporation

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
from urllib3.connectionpool import InsecureRequestWarning
import yaml
from dbt.tests.util import (
    run_dbt,
    read_file,
    write_file,
)
from tests.fixtures.profiles import unique_schema, dbt_profile_data

verify_ssl_model = """
select * from Samples."samples.dremio.com"."NYC-taxi-trips-iceberg" limit 10
"""


class TestVerifyCertificateDremio:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_verify_ssl.sql": verify_ssl_model,
        }

    def update_config_file(self, updates, *paths):
        current_yaml = read_file(*paths)
        config = yaml.safe_load(current_yaml)
        config["test"]["outputs"]["default"].update(updates)
        new_yaml = yaml.safe_dump(config)
        write_file(new_yaml, *paths)

    def test_verify_cert(self, project):
        with pytest.warns(InsecureRequestWarning):
            self.update_config_file(
                {"verify_ssl": False},
                project.profiles_dir,
                "profiles.yml",
            )

            run_dbt(["run"])
