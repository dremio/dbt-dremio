import agate
from dbt.adapters.base.meta import available
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

    def create_schema(self, relation: DremioRelation) -> None:
        database = relation.database
        schema = relation.schema
        logger.debug('Creating schema "{}.{}".', database, schema)
        DremioConnectionManager.create_catalog(database, schema)
