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
from dbt.tests.adapter.relations.test_changing_relation_type import (
    BaseChangeRelationTypeValidator,
)
from tests.fixtures.profiles import unique_schema, dbt_profile_data


_DEFAULT_CHANGE_RELATION_TYPE_MODEL = """
{{ config(
    materialized=var('materialized'),
    twin_strategy='prevent',
) }}

select '{{ var("materialized") }}' as materialization

{% if var('materialized') == 'incremental' and is_incremental() %}
    where 'abc' != (select max(materialization) from {{ this }})
{% endif %}
"""

class TestChangeRelationTypesDremio(BaseChangeRelationTypeValidator):
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_mc_modelface.sql": _DEFAULT_CHANGE_RELATION_TYPE_MODEL}
