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

from dbt.adapters.sql import SQLAdapter
from dbt.adapters.dremio import DremioConnectionManager
from dbt.adapters.dremio.relation import DremioRelation

from typing import List
from typing import Optional
import dbt.flags
from dbt.adapters.base.relation import BaseRelation
#from dbt.logger import GLOBAL_LOGGER as logger
from dbt.events import AdapterLogger
logger = AdapterLogger("dremio")

from dbt.adapters.base.meta import available

import agate

class DremioAdapter(SQLAdapter):
    ConnectionManager = DremioConnectionManager
    Relation = DremioRelation

    @classmethod
    def date_function(cls):
        return 'current_date'

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return "varchar"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "timestamp"

    @classmethod
    def convert_date_type(cls, agate_table, col_idx):
        return "date"

    @classmethod
    def convert_boolean_type(cls, agate_table, col_idx):
        return "boolean"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "decimal" if decimals else "bigint"

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "time"
