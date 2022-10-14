import agate
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.dremio import DremioConnectionManager
from dbt.adapters.dremio.relation import DremioRelation


from typing import List
from typing import Optional
from dbt.adapters.base.relation import BaseRelation

from dbt.events import AdapterLogger

logger = AdapterLogger("dremio")


class DremioAdapter(SQLAdapter):
    ConnectionManager = DremioConnectionManager
    Relation = DremioRelation

    @classmethod
    def date_function(cls):
        return "current_date"

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
        self.connections.create_catalog(database, schema)

    def drop_schema(self, relation: DremioRelation) -> None:
        database = relation.database
        schema = relation.schema
        self.connections.drop_catalog(database, schema)

    def timestamp_add_sql(
        self, add_to: str, number: int = 1, interval: str = "hour"
    ) -> str:
        return f"DATE_ADD({add_to}, CAST({number} AS INTERVAL {interval}))"

    def get_rows_different_sql(
        self,
        relation_a: BaseRelation,
        relation_b: BaseRelation,
        column_names: Optional[List[str]] = ["*"],
        except_operator: str = "EXCEPT",
    ) -> str:
        """Generate SQL for a query that returns a single row with a two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """
        # This method only really exists for test reasons.
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))
        columns_csv = ", ".join(names)

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
            except_op=except_operator,
        )

        return sql


COLUMNS_EQUAL_SQL = """
with diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            (SELECT {columns} FROM {relation_a} {except_op}
             SELECT {columns} FROM {relation_b})
             UNION ALL
            (SELECT {columns} FROM {relation_b} {except_op}
             SELECT {columns} FROM {relation_a})
        ) as a
), table_a as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), table_b as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        table_a.num_rows - table_b.num_rows as difference
    from table_a, table_b
)
select
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
from row_count_diff
join diff_count using (id)
""".strip()
