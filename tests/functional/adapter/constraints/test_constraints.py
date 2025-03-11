import pytest
from dbt.adapters.dremio.api.rest.client import DremioRestClient

from dbt.tests.adapter.constraints.test_constraints import (
    BaseConstraintQuotedColumn,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseModelConstraintsRuntimeEnforcement,
    BaseIncrementalConstraintsColumnsEqual,
    BaseIncrementalConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
)

from tests.utils.util import BUCKET

_expected_sql_dremio = """
create table <model_identifier> ( id integer not null, color VARCHAR, date_day VARCHAR ) as ( select id, color, date_day from ( -- depends_on: <foreign_key_model_identifier> select 'blue' as color, 1 as id, '2019-01-01' as date_day ) as model_subq )

"""


class DremioColumnEqualSetup:
    @pytest.fixture
    def string_type(self):
        return "VARCHAR"

    @pytest.fixture
    def int_type(self):
        return "INTEGER"

    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["cast('2019-01-01' as date)", "date", "DATE"],
            ["true", "boolean", "BOOLEAN"],
            ["cast('2013-11-03 00:00:00-07' as TIMESTAMP)", "timestamp(6)", "TIMESTAMP"],
            ["cast('1' as DECIMAL)", "DECIMAL", "DECIMAL"],
        ]

    @pytest.fixture(scope="class")
    def dbt_profile_data(
            self, unique_schema, dbt_profile_target, profiles_config_update
    ):
        profile = {
            "test": {
                "outputs": {
                    "default": {},
                },
                "target": "default",
            },
        }
        target = dbt_profile_target
        target["schema"] = unique_schema
        target["root_path"] = f"{BUCKET}.{unique_schema}"
        profile["test"]["outputs"]["default"] = target

        if profiles_config_update:
            profile.update(profiles_config_update)
        return profile

class TestDremioTableConstraintsColumnsEqual(
    DremioColumnEqualSetup, BaseTableConstraintsColumnsEqual
):
    pass

class TestDremioViewConstraintsColumnsEqual(DremioColumnEqualSetup, BaseViewConstraintsColumnsEqual):
    pass

class TestDremioTableConstraintsRuntimeDdlEnforcement(DremioColumnEqualSetup, BaseConstraintsRuntimeDdlEnforcement):
    @pytest.fixture(scope="class")
    def dbt_profile_data(
            self, unique_schema, dbt_profile_target, profiles_config_update
    ):
        profile = {
            "test": {
                "outputs": {
                    "default": {},
                },
                "target": "default",
            },
        }
        target = dbt_profile_target
        target["schema"] = unique_schema
        target["root_path"] = f"{BUCKET}.{unique_schema}"
        profile["test"]["outputs"]["default"] = target

        if profiles_config_update:
            profile.update(profiles_config_update)
        return profile

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql_dremio

# TODO: Revisit once at least one of the constraints starts being enforced
@pytest.mark.skip("Dremio does not enforce any constraints so rollbacks can't be tested")
class TestDremioTableConstraintsRollback(BaseConstraintsRollback):
    pass

class TestDremioIncrementalConstraintsColumnsEqual(DremioColumnEqualSetup, BaseIncrementalConstraintsColumnsEqual):
    pass

# TODO: Revisit once at least one of the constraints starts being enforced
@pytest.mark.skip("Dremio does not enforce any constraints so rollbacks can't be tested")
class TestDremioIncrementalConstraintsRollback(BaseIncrementalConstraintsRollback):
    pass

class TestDremioIncrementalConstraintsRuntimeDdlEnforcement(DremioColumnEqualSetup, BaseIncrementalConstraintsRuntimeDdlEnforcement):
    @pytest.fixture(scope="class")
    def dbt_profile_data(
            self, unique_schema, dbt_profile_target, profiles_config_update
    ):
        profile = {
            "test": {
                "outputs": {
                    "default": {},
                },
                "target": "default",
            },
        }
        target = dbt_profile_target
        target["schema"] = unique_schema
        target["root_path"] = f"{BUCKET}.{unique_schema}"
        profile["test"]["outputs"]["default"] = target

        if profiles_config_update:
            profile.update(profiles_config_update)
        return profile

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
create table <model_identifier> ( id integer not null, color VARCHAR, date_day VARCHAR ) as ( select id, color, date_day from ( -- depends_on: <foreign_key_model_identifier> select 'blue' as color, 1 as id, '2019-01-01' as date_day ) as model_subq )
"""

class TestDremioModelConstraintsRuntimeEnforcement(DremioColumnEqualSetup, BaseModelConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
create table <model_identifier> ( id integer not null, color VARCHAR, date_day VARCHAR ) as ( select id, color, date_day from ( -- depends_on: <foreign_key_model_identifier> select 'blue' as color, 1 as id, '2019-01-01' as date_day ) as model_subq )
"""

class TestDremioConstraintQuotedColumn(DremioColumnEqualSetup, BaseConstraintQuotedColumn):
    @pytest.fixture(scope="class")
    def dbt_profile_data(
            self, unique_schema, dbt_profile_target, profiles_config_update
    ):
        profile = {
            "test": {
                "outputs": {
                    "default": {},
                },
                "target": "default",
            },
        }
        target = dbt_profile_target
        target["schema"] = unique_schema
        target["root_path"] = f"{BUCKET}.{unique_schema}"
        profile["test"]["outputs"]["default"] = target

        if profiles_config_update:
            profile.update(profiles_config_update)
        return profile

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
create table <model_identifier> (
    id integer not null,
    "from" varchar not null,
    date_day varchar
    
    )   
  as (
    
    select id, "from", date_day
    from (
        select
  'blue' as "from",
  1 as id,
  '2019-01-01' as date_day
    ) as model_subq
  )
  
"""