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

import pytest, os

from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from tests.functional.adapter.grants.base_grants import BaseGrantsDremio
from tests.utils.util import relation_from_name
from dbt.tests.util import get_connection

DREMIO_EDITION = os.getenv("DREMIO_EDITION")

@pytest.mark.skipif(DREMIO_EDITION == "community", reason="Dremio only supports grants in EE/DC editions.")
class TestInvalidGrantsDremio(BaseGrantsDremio, BaseInvalidGrants):
    def grantee_does_not_exist_error(self):
        return "Grant on catalog entity failed. User invalid_user does not exist."

    def privilege_does_not_exist_error(self):
        return 'Encountered "fake_privilege"'
