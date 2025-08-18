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
import pytest
from typing import Dict
import os

from dbt.config.profile import read_profile
from dbt.adapters.dremio.credentials import DremioCredentials
from tests.utils.util import SOURCE

DREMIO_EDITION = os.getenv("DREMIO_EDITION")

# Tests require manual setup before executing.
#
#     Prior to running these tests, create four dbt projects:
#
#     1. `dbt init test_cloud_options`
#         - accept all default options
#         - provide any value for mandatory options
#
#     2. `dbt init test_sw_up_options`
#         - accept all default options
#         - provide any value for mandatory options
#
#     3. `dbt init test_sw_pat_options`
#         - accept all default options
#         - provide any value for mandatory options
#
#     4. `dbt init test_enterprise_catalog_options`
#         - select enterprise catalog storage option
#         - accept all default options
#         - provide any value for mandatory options
#
#     These tests assumes there exists a $HOME/.dbt/profiles.yml
#     file containing these four dbt projects.


class TestProfileTemplate:
    # non-OS specific
    PROFILE_DIRECTORY = os.path.expanduser("~") + "/.dbt/"

    # These projects must exist in the profile.yml file. All defaults must be selected.
    _TEST_CLOUD_PROFILE_PROJECT = (
        "test_cloud_options"  # nosec hardcoded_password_string
    )
    _TEST_SOFTWARE_USER_PASSWORD_PROFILE_PROJECT = (
        "test_sw_up_options"  # nosec hardcoded_password_string
    )
    _TEST_SOFTWARE_PAT_PROFILE_PROJECT = "test_sw_pat_options"
    _TEST_ENTERPRISE_CATALOG_PROFILE_PROJECT = "test_enterprise_catalog_options"

    _PASSWORD_AUTH_PROFILE_OPTIONS_WITH_DEFAULTS = {"password": None}
    _PAT_AUTH_PROFILE_OPTIONS_WITH_DEFAULTS = {"pat": None}
    _COMMON_PROFILE_OPTIONS_WITH_DEFAULTS = {
        "user": None,
        "threads": 1,
    }
    _DREMIO_CLOUD_PROFILE_SPECIFIC_OPTIONS_WITH_DEFAULTS = {
        "dremio_space": SOURCE,
        "dremio_space_folder": "space",
        "object_storage_source": SOURCE,
        "object_storage_path": "object_storage",
        "use_ssl": False,
    }
    _DREMIO_SW_PROFILE_SPECIFIC_OPTIONS_WITH_DEFAULTS = {
        "object_storage_source": "$scratch",
        "software_host": None,
        "port": 9047,
        "use_ssl": False,
    }
    _ENTERPRISE_CATALOG_PROFILE_SPECIFIC_OPTIONS_WITH_DEFAULTS = {
        "enterprise_catalog_namespace": None,
        "enterprise_catalog_folder": None,
        "software_host": None,
        "port": 9047,
        "use_ssl": False,
    }

    _DREMIO_CLOUD_PROFILE_OPTIONS_WITH_DEFAULTS = (
        _COMMON_PROFILE_OPTIONS_WITH_DEFAULTS
        | _DREMIO_CLOUD_PROFILE_SPECIFIC_OPTIONS_WITH_DEFAULTS
        | _PAT_AUTH_PROFILE_OPTIONS_WITH_DEFAULTS
    )
    _DREMIO_SOFTWARE_USERNAME_PASSWORD_PROFILE_OPTIONS = (
        _COMMON_PROFILE_OPTIONS_WITH_DEFAULTS
        | _DREMIO_SW_PROFILE_SPECIFIC_OPTIONS_WITH_DEFAULTS
        | _PASSWORD_AUTH_PROFILE_OPTIONS_WITH_DEFAULTS
    )
    _DREMIO_SOFTWARE_PAT_PROFILE_OPTIONS = (
        _COMMON_PROFILE_OPTIONS_WITH_DEFAULTS
        | _DREMIO_SW_PROFILE_SPECIFIC_OPTIONS_WITH_DEFAULTS
        | _PAT_AUTH_PROFILE_OPTIONS_WITH_DEFAULTS
    )
    _DREMIO_ENTERPRISE_CATALOG_USERNAME_PASSWORD_PROFILE_OPTIONS = (
        _COMMON_PROFILE_OPTIONS_WITH_DEFAULTS
        | _ENTERPRISE_CATALOG_PROFILE_SPECIFIC_OPTIONS_WITH_DEFAULTS
        | _PASSWORD_AUTH_PROFILE_OPTIONS_WITH_DEFAULTS
    )
    _DREMIO_ENTERPRISE_CATALOG_PAT_PROFILE_OPTIONS = (
        _COMMON_PROFILE_OPTIONS_WITH_DEFAULTS
        | _ENTERPRISE_CATALOG_PROFILE_SPECIFIC_OPTIONS_WITH_DEFAULTS
        | _PAT_AUTH_PROFILE_OPTIONS_WITH_DEFAULTS
    )

    _PROFILE_OPTIONS_ALIASES = {
        "username": "UID",
        "user": "UID",
        "password": "PWD",
        "object_storage_source": "datalake",
        "object_storage_path": "root_path",
        "dremio_space": "database",
        "dremio_space_folder": "schema",
    }

    @pytest.mark.skipif(
        DREMIO_EDITION == "community",
        reason="Cloud options are not available in Dremio community edition.",
    )
    def test_cloud_options(self) -> None:
        self._test_project_profile_options(
            self._get_dbt_test_project_dict(self._TEST_CLOUD_PROFILE_PROJECT),
            self._DREMIO_CLOUD_PROFILE_OPTIONS_WITH_DEFAULTS,
        )

    def test_software_username_password_options(self) -> None:
        self._test_project_profile_options(
            self._get_dbt_test_project_dict(
                self._TEST_SOFTWARE_USER_PASSWORD_PROFILE_PROJECT
            ),
            self._DREMIO_SOFTWARE_USERNAME_PASSWORD_PROFILE_OPTIONS,
        )

    def test_software_pat_options(self) -> None:
        self._test_project_profile_options(
            self._get_dbt_test_project_dict(self._TEST_SOFTWARE_PAT_PROFILE_PROJECT),
            self._DREMIO_SOFTWARE_PAT_PROFILE_OPTIONS,
        )

    def test_aliases(self) -> None:
        credentials = DremioCredentials()
        credential_option_aliases = credentials.aliases
        for option, alias in self._PROFILE_OPTIONS_ALIASES.items():
            assert credential_option_aliases[option] is not None
            assert alias == credential_option_aliases[option]


    @pytest.mark.skipif(
        DREMIO_EDITION == "community",
        reason="Enterprise catalog options are not available in Dremio community edition.",
    )
    def test_enterprise_catalog_options(self) -> None:
        self._test_project_profile_options(
            self._get_dbt_test_project_dict(self._TEST_ENTERPRISE_CATALOG_PROFILE_PROJECT),
            self._DREMIO_ENTERPRISE_CATALOG_PROFILE_OPTIONS,
        )

    @pytest.mark.skip
    def _get_dbt_test_project_dict(self, dbt_test_project_name: str) -> Dict[str, any]:
        # read_profile returns dictionary with the following layout:
        # { <project name>: { 'outputs': { 'dev' } : { <all the profile options and values that we want to test> ] } }
        profile_dictionary = read_profile(TestProfileTemplate.PROFILE_DIRECTORY)
        return profile_dictionary.get(dbt_test_project_name).get("outputs").get("dev")

    @pytest.mark.skip
    def _test_project_profile_options(
        self, test_project: Dict[str, any], test_options: Dict[str, any]
    ) -> None:
        assert test_project is not None

        for option in test_options:
            assert test_project[option] is not None
            if test_options[option] is not None:
                assert test_project[option] == test_options[option]

class TestProfileValidation:
    @pytest.mark.skipif(
        DREMIO_EDITION == "community",
        reason="Enterprise catalog options are not available in Dremio community edition.",
    )
    def test_invalid_enterprise_catalog_profile(self) -> None:
        """Test that using both enterprise catalog and space & source configs raises validation error."""
        from dbt_common.exceptions import DbtValidationError

        # Simulate profile data with both enterprise catalog and individual storage configurations
        conflicting_data = {
            "enterprise_catalog_namespace": "my_enterprise_catalog",
            "enterprise_catalog_folder": "analytics_folder",
            "datalake": "my_source",
            "root_path": "my_path",
            "database": "my_space",
            "schema": "my_schema",
            "user": "test_user",
            "pat": "test_token",
            "threads": 1,
            "type": "dremio"
        }

        # Verify that validation raises an error for conflicting configurations
        with pytest.raises(DbtValidationError, match="Cannot use both enterprise catalog and individual storage configurations"):
            DremioCredentials._validate_and_restructure_data(conflicting_data)
