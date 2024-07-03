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

from dbt.tests.adapter.utils.test_generate_series import BaseGenerateSeries
from tests.utils.util import BUCKET
from tests.fixtures.profiles import unique_schema, dbt_profile_data


class TestGenerateSeriesDremio(BaseGenerateSeries):
    pass
