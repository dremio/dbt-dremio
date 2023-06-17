import os
import pytest

from pathlib import Path

from tests.hooks.fixtures import (
    macros__hook,
    macros__before_and_after,
    models__hooks,
    seeds__example_seed_csv,
    macros_missing_column,
    models__missing_column,
)

from dbt.tests.util import (
    check_table_does_not_exist,
    run_dbt,
)

from tests.utils.util import BUCKET, SOURCE


# This ensures the schema works with our datalake
@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    test_file = request.module.__name__
    # We only want the last part of the name
    test_file = test_file.split(".")[-1]
    unique_schema = f"{BUCKET}.{prefix}_{test_file}"
    return unique_schema


# Override this fixture to set root_path=schema
@pytest.fixture(scope="class")
def dbt_profile_data(unique_schema, dbt_profile_target, profiles_config_update):
    profile = {
        "config": {"send_anonymous_usage_stats": False},
        "test": {
            "outputs": {
                "default": {},
            },
            "target": "default",
        },
    }
    target = dbt_profile_target
    target["schema"] = unique_schema
    target["root_path"] = f"{unique_schema}"
    profile["test"]["outputs"]["default"] = target

    if profiles_config_update:
        profile.update(profiles_config_update)
    return profile


class TestPrePostRunHooks(object):
    @pytest.fixture(scope="function")
    def setUp(self, project):
        project.run_sql_file(project.test_data_dir / Path("seed_run.sql"))
        project.run_sql(
            f"drop table if exists {SOURCE}.{ project.test_schema }.run_hooks_schemas"
        )
        project.run_sql(
            f"drop table if exists {SOURCE}.{ project.test_schema }.db_schemas"
        )
        os.environ["TERM_TEST"] = "TESTING"

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "hook.sql": macros__hook,
            "before-and-after.sql": macros__before_and_after,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": models__hooks}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"example_seed.csv": seeds__example_seed_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            # The create and drop table statements here validate that these hooks run
            # in the same order that they are defined. Drop before create is an error.
            # Also check that the table does not exist below.
            "on-run-start": [
                "{{ custom_run_hook('start', target, run_started_at, invocation_id) }}",
                f"create table {SOURCE}.{{{{ target.schema }}}}.start_hook_order_test ( id int )",
                f"drop table {SOURCE}.{{{{ target.schema }}}}.start_hook_order_test",
                "{{ log(env_var('TERM_TEST'), info=True) }}",
            ],
            "on-run-end": [
                "{{ custom_run_hook('end', target, run_started_at, invocation_id) }}",
                f"create table {SOURCE}.{{{{ target.schema }}}}.end_hook_order_test ( id int )",
                f"drop table {SOURCE}.{{{{ target.schema }}}}.end_hook_order_test",
                f"create table {SOURCE}.{{{{ target.schema }}}}.run_hooks_schemas ( schema VARCHAR )",
                f"insert into {SOURCE}.{{{{ target.schema }}}}.run_hooks_schemas (schema) values {{% for schema in schemas %}}( '{{{{ schema }}}}' ){{% if not loop.last %}},{{% endif %}}{{% endfor %}}",
                f"create table {SOURCE}.{{{{ target.schema }}}}.db_schemas ( db VARCHAR, schema VARCHAR )",
                f"insert into {SOURCE}.{{{{ target.schema }}}}.db_schemas (db, schema) values {{% for db, schema in database_schemas %}}('{{{{ db }}}}', '{{{{ schema }}}}' ){{% if not loop.last %}},{{% endif %}}{{% endfor %}}",
            ],
            "seeds": {
                "quote_columns": False,
            },
        }

    def get_ctx_vars(self, state, project):
        fields = [
            "test_state",
            "target_dbname",
            "target_host",
            "target_name",
            "target_schema",
            "target_threads",
            "target_type",
            "target_user",
            "target_pass",
            "run_started_at",
            "invocation_id",
        ]
        field_list = ", ".join(['"{}"'.format(f) for f in fields])
        query = f"select {field_list} from {SOURCE}.{project.test_schema}.on_run_hook where test_state = '{state}'"

        vals = project.run_sql(query, fetch="all")
        assert len(vals) != 0, "nothing inserted into on_run_hook table"
        assert len(vals) == 1, "too many rows in hooks table"
        ctx = dict([(k, v) for (k, v) in zip(fields, vals[0])])

        return ctx

    def assert_used_schemas(self, project):
        schemas_query = (
            f"select * from {SOURCE}.{project.test_schema}.run_hooks_schemas"
        )
        results = project.run_sql(schemas_query, fetch="all")
        assert len(results) == 1
        assert results[0][0] == project.test_schema

        db_schemas_query = f"select * from {SOURCE}.{project.test_schema}.db_schemas"
        results = project.run_sql(db_schemas_query, fetch="all")
        assert len(results) == 1
        assert results[0][0] == project.database or results[0][0] == SOURCE
        assert results[0][1] == project.test_schema

    def check_hooks(self, state, project, host):
        ctx = self.get_ctx_vars(state, project)

        assert ctx["test_state"] == state
        assert ctx["target_dbname"] is None
        assert ctx["target_host"] is None
        assert ctx["target_name"] == "default"
        assert ctx["target_schema"] == f"{SOURCE}.{project.test_schema}"
        assert ctx["target_threads"] == 1
        assert ctx["target_type"] == "dremio"
        assert ctx["target_user"] == os.getenv("DREMIO_CLOUD_USERNAME")
        assert ctx["target_pass"] is None

        assert (
            ctx["run_started_at"] is not None and len(ctx["run_started_at"]) > 0
        ), "run_started_at was not set"
        assert (
            ctx["invocation_id"] is not None and len(ctx["invocation_id"]) > 0
        ), "invocation_id was not set"

    def test_pre_and_post_run_hooks(self, setUp, project, dbt_profile_target):
        run_dbt(["run"])

        self.check_hooks("start", project, dbt_profile_target.get("host", None))
        self.check_hooks("end", project, dbt_profile_target.get("host", None))

        check_table_does_not_exist(project.adapter, "start_hook_order_test")
        check_table_does_not_exist(project.adapter, "end_hook_order_test")
        self.assert_used_schemas(project)

    def test_pre_and_post_seed_hooks(self, setUp, project, dbt_profile_target):
        run_dbt(["seed"])

        self.check_hooks("start", project, dbt_profile_target.get("host", None))
        self.check_hooks("end", project, dbt_profile_target.get("host", None))

        check_table_does_not_exist(project.adapter, "start_hook_order_test")
        check_table_does_not_exist(project.adapter, "end_hook_order_test")
        self.assert_used_schemas(project)


class TestAfterRunHooks(object):
    @pytest.fixture(scope="class")
    def macros(self):
        return {"temp_macro.sql": macros_missing_column}

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_column.sql": models__missing_column}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            # The create and drop table statements here validate that these hooks run
            # in the same order that they are defined. Drop before create is an error.
            # Also check that the table does not exist below.
            "on-run-start": "- {{ export_table_check() }}"
        }

    def test_missing_column_pre_hook(self, project):
        run_dbt(["run"], expect_pass=False)
