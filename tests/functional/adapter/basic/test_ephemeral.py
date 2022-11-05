import pytest
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.ephemeral.test_ephemeral import BaseEphemeralMulti
from tests.functional.adapter.utils.test_utils import DATALAKE
from dbt.tests.util import run_dbt
from tests.fixtures.profiles import unique_schema, dbt_profile_data


class TestEphemeralDremio(BaseEphemeral):
    pass


class TestEphemeralMulti(BaseEphemeralMulti):
    def test_ephemeral_multi(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 3

        check_relations_equal(project.adapter, ["seed", "dependent"])
        check_relations_equal(project.adapter, ["seed", "double_dependent"])
        check_relations_equal(project.adapter, ["seed", "super_dependent"])
        assert os.path.exists("./target/run/test/models/double_dependent.sql")
        with open("./target/run/test/models/double_dependent.sql", "r") as fp:
            sql_file = fp.read()

        sql_file = re.sub(r"\d+", "", sql_file)
        expected_sql = (
            'create view "dbt"."test_test_ephemeral"."double_dependent__dbt_tmp" as ('
            "with __dbt__cte__base as ("
            "select * from test_test_ephemeral.seed"
            "),  __dbt__cte__base_copy as ("
            "select * from __dbt__cte__base"
            ")-- base_copy just pulls from base. Make sure the listed"
            "-- graph of CTEs all share the same dbt_cte__base cte"
            "select * from __dbt__cte__base where gender = 'Male'"
            "union all"
            "select * from __dbt__cte__base_copy where gender = 'Female'"
            ");"
        )
        sql_file = "".join(sql_file.split())
        expected_sql = "".join(expected_sql.split())
        assert sql_file == expected_sql
