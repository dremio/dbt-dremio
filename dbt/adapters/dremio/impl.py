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

import agate
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.dremio import DremioConnectionManager
from dbt.adapters.dremio.relation import DremioRelation
from typing import Dict

from typing import List
from typing import Optional
from dbt.adapters.base.meta import available
from dbt.adapters.base.relation import BaseRelation

from dbt.adapters.capability import (
    CapabilityDict,
    CapabilitySupport,
    Support,
    Capability,
)
from dbt.adapters.sql.impl import DROP_RELATION_MACRO_NAME
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("dremio")


class DremioAdapter(SQLAdapter):
    ConnectionManager = DremioConnectionManager
    Relation = DremioRelation

    _capabilities = CapabilityDict(
        {
            Capability.TableLastModifiedMetadata: CapabilitySupport(
                support=Support.Full
            ),
            Capability.SchemaMetadataByRelations: CapabilitySupport(
                support=Support.Full
            ),
        }
    )

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
        self.connections.create_catalog(relation)

    def drop_schema(self, relation: DremioRelation) -> None:
        if relation.type == "table":
            self.execute_macro(DROP_RELATION_MACRO_NAME, kwargs={"relation": relation})

        else:
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

    def valid_incremental_strategies(self):
        """The set of standard builtin strategies which this adapter supports out-of-the-box.
        Not used to validate custom strategies defined by end users.
        """
        return ["append"]

    def standardize_grants_dict(self, grants_table: agate.Table) -> dict:
        """Translate the result of `show grants` (or equivalent) to match the
        grants which a user would configure in their project.

        Ideally, the SQL to show grants should also be filtering:
        filter OUT any grants TO the current user/role (e.g. OWNERSHIP).
        If that's not possible in SQL, it can be done in this method instead.

        :param grants_table: An agate table containing the query result of
            the SQL returned by get_show_grant_sql
        :return: A standardized dictionary matching the `grants` config
        :rtype: dict
        """
        grants_dict: Dict[str, List[str]] = {}
        for row in grants_table:
            # Just needed to change these two values to match Dremio cols
            grantee = row["grantee_id"]
            privilege = row["privilege"]
            if privilege in grants_dict.keys():
                grants_dict[privilege].append(grantee)
            else:
                grants_dict.update({privilege: [grantee]})
        return grants_dict

    # This is for use in the test suite
    # Need to override to add fetch to the execute method
    def run_sql_for_tests(self, sql, fetch, conn):
        cursor = conn.handle.cursor()
        try:
            cursor.execute(sql, None, True)
            if hasattr(conn.handle, "commit"):
                conn.handle.commit()
            if fetch == "one":
                return cursor.fetchone()
            elif fetch == "all":
                return cursor.fetchall()
            else:
                return
        except BaseException as e:
            if conn.handle and not getattr(conn.handle, "closed", True):
                conn.handle.rollback()
            print(sql)
            print(e)
            raise
        finally:
            conn.transaction_open = False

    # dbt docs integration with Dremio wikis and tags
    @available
    def process_wikis(self, relation: DremioRelation, text: str) -> None:
        self.connections.process_wikis(relation, text)

    @available
    def process_tags(self, relation: DremioRelation, tags: list[str]) -> None:
        self.connections.process_tags(relation, tags)

    @available
    def create_reflection(self, name: str, type: str, anchor: DremioRelation, display: List[str], dimensions: List[str],
                          date_dimensions: List[str], measures: List[str], computations: List[str],
                          partition_by: List[str], partition_transform: List[str], partition_method: str,
                          distribute_by: List[str], localsort_by: List[str], arrow_cache: bool) -> None:
        self.connections.create_reflection(name, type, anchor, display, dimensions, date_dimensions, measures,
                                           computations, partition_by, partition_transform, partition_method,
                                           distribute_by, localsort_by, arrow_cache)


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
