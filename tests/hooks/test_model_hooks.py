import os
import pytest

from pathlib import Path

from dbt.exceptions import CompilationError, ParsingError

from dbt.tests.util import (
    run_dbt,
    write_file,
)

from dbt.tests.adapter.hooks.fixtures import (
    properties__seed_models,
    properties__test_snapshot_models,
    seeds__example_seed_csv,
)

from tests.hooks.fixtures import (
    models__hooked,
    models__hooks,
    models__hooks_configured,
    models__hooks_error,
    models__hooks_kwargs,
    models__post,
    models__pre,
    snapshots__test_snapshot,
)

from tests.utils.util import BUCKET, SOURCE


# This ensures the schema works with our datalake
@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    test_file = request.module.__name__
    # We only want the last part of the name
    test_file = test_file.split(".")[-1]
    unique_schema = f"{BUCKET}.{test_file}"
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


MODEL_PRE_HOOK = f"""
   insert into {SOURCE}.{{{{this.schema}}}}.on_model_hook (
        test_state,
        target_dbname,
        target_host,
        target_name,
        target_schema,
        target_type,
        target_user,
        target_pass,
        target_threads,
        run_started_at,
        invocation_id
   ) VALUES (
    'start',
    '{{{{ target.dbname }}}}',
    '{{{{ target.host }}}}',
    '{{{{ target.name }}}}',
    '{SOURCE}.{{{{ target.schema }}}}',
    '{{{{ target.type }}}}',
    '{{{{ target.user }}}}',
    '{{{{ target.get("pass", "") }}}}',
    {{{{ target.threads }}}},
    '{{{{ run_started_at }}}}',
    '{{{{ invocation_id }}}}'
   )
"""

MODEL_POST_HOOK = f"""
   insert into {SOURCE}.{{{{this.schema}}}}.on_model_hook (
        test_state,
        target_dbname,
        target_host,
        target_name,
        target_schema,
        target_type,
        target_user,
        target_pass,
        target_threads,
        run_started_at,
        invocation_id
   ) VALUES (
    'end',
    '{{{{ target.dbname }}}}',
    '{{{{ target.host }}}}',
    '{{{{ target.name }}}}',
    '{SOURCE}.{{{{ target.schema }}}}',
    '{{{{ target.type }}}}',
    '{{{{ target.user }}}}',
    '{{{{ target.get("pass", "") }}}}',
    {{{{ target.threads }}}},
    '{{{{ run_started_at }}}}',
    '{{{{ invocation_id }}}}'
   )
"""


class BaseTestPrePost(object):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql_file(project.test_data_dir / Path("seed_model.sql"))

    def get_ctx_vars(self, state, count, project):
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
        query = f"select {field_list} from {SOURCE}.{project.test_schema}.on_model_hook where test_state = '{state}'"

        vals = project.run_sql(query, fetch="all")
        assert len(vals) != 0, "nothing inserted into hooks table"
        assert len(vals) >= count, "too few rows in hooks table"
        assert len(vals) <= count, "too many rows in hooks table"
        return [{k: v for k, v in zip(fields, val)} for val in vals]

    def check_hooks(self, state, project, host, count=1):
        ctxs = self.get_ctx_vars(state, count=count, project=project)
        for ctx in ctxs:
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


class TestPrePostModelHooks(BaseTestPrePost):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre-hook": [
                        # inside transaction (runs second)
                        MODEL_PRE_HOOK,
                        # outside transaction (runs first)
                        {
                            "sql": "vacuum {{ this.schema }}.on_model_hook",
                            "transaction": False,
                        },
                    ],
                    "post-hook": [
                        # outside transaction (runs second)
                        {
                            "sql": "vacuum {{ this.schema }}.on_model_hook",
                            "transaction": False,
                        },
                        # inside transaction (runs first)
                        MODEL_POST_HOOK,
                    ],
                }
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": models__hooks}

    def test_pre_and_post_run_hooks(self, project, dbt_profile_target):
        run_dbt()
        self.check_hooks("start", project, dbt_profile_target.get("host", None))
        self.check_hooks("end", project, dbt_profile_target.get("host", None))


class TestPrePostModelHooksUnderscores(TestPrePostModelHooks):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre_hook": [
                        # inside transaction (runs second)
                        MODEL_PRE_HOOK,
                        # outside transaction (runs first)
                        {
                            "sql": "vacuum {{ this.schema }}.on_model_hook",
                            "transaction": False,
                        },
                    ],
                    "post_hook": [
                        # outside transaction (runs second)
                        {
                            "sql": "vacuum {{ this.schema }}.on_model_hook",
                            "transaction": False,
                        },
                        # inside transaction (runs first)
                        MODEL_POST_HOOK,
                    ],
                }
            }
        }


class TestHookRefs(BaseTestPrePost):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "hooked": {
                        "post-hook": [
                            f"""
                        insert into {SOURCE}.{{{{this.schema}}}}.on_model_hook select
                        test_state,
                        '{{{{ target.dbname }}}}' as target_dbname,
                        '{{{{ target.host }}}}' as target_host,
                        '{{{{ target.name }}}}' as target_name,
                        '{SOURCE}.{{{{ target.schema }}}}' as target_schema,
                        '{{{{ target.type }}}}' as target_type,
                        '{{{{ target.user }}}}' as target_user,
                        '{{{{ target.get(pass, "") }}}}' as target_pass,
                        {{{{ target.threads }}}} as target_threads,
                        '{{{{ run_started_at }}}}' as run_started_at,
                        '{{{{ invocation_id }}}}' as invocation_id
                        from {{{{ ref('post') }}}}""".strip()
                        ],
                    }
                },
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hooked.sql": models__hooked,
            "post.sql": models__post,
            "pre.sql": models__pre,
        }

    def test_pre_post_model_hooks_refed(self, project, dbt_profile_target):
        run_dbt()
        self.check_hooks("start", project, dbt_profile_target.get("host", None))
        self.check_hooks("end", project, dbt_profile_target.get("host", None))


class TestPrePostModelHooksOnSeeds(object):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"example_seed.csv": seeds__example_seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": properties__seed_models}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "models": {},
            "seeds": {
                "post-hook": [
                    "alter table {{ this }} add columns (new_col int)",
                    "update {{ this }} set new_col = 1",
                    # call any macro to track dependency: https://github.com/dbt-labs/dbt-core/issues/6806
                    "select CAST(1 AS {{ dbt.type_int() }}) as id",
                ],
                "quote_columns": False,
            },
        }

    def test_hooks_on_seeds(self, project):
        res = run_dbt(["seed"])
        assert len(res) == 1, "Expected exactly one item"
        res = run_dbt(["test"])
        assert len(res) == 1, "Expected exactly one item"


class TestHooksRefsOnSeeds:
    """
    This should not succeed, and raise an explicit error
    https://github.com/dbt-labs/dbt-core/issues/6806
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"example_seed.csv": seeds__example_seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": properties__seed_models, "post.sql": models__post}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "post-hook": [
                    "select * from {{ ref('post') }}",
                ],
            },
        }

    def test_hook_with_ref_on_seeds(self, project):
        with pytest.raises(ParsingError) as excinfo:
            run_dbt(["parse"])
        assert "Seeds cannot depend on other nodes" in str(excinfo.value)


class TestPrePostModelHooksOnSeedsPlusPrefixed(TestPrePostModelHooksOnSeeds):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "models": {},
            "seeds": {
                "+post-hook": [
                    "alter table {{ this }} add columns (new_col int)",
                    "update {{ this }} set new_col = 1",
                ],
                "quote_columns": False,
            },
        }


class TestPrePostModelHooksOnSeedsPlusPrefixedWhitespace(TestPrePostModelHooksOnSeeds):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "models": {},
            "seeds": {
                "+post-hook": [
                    "alter table {{ this }} add columns (new_col int)",
                    "update {{ this }} set new_col = 1",
                ],
                "quote_columns": False,
            },
        }


class TestPrePostModelHooksOnSnapshots(object):
    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        # We only want the last part of the name
        test_file = test_file.split(".")[-1]
        unique_schema = f"{BUCKET}.{prefix}_{test_file}"
        return unique_schema

    @pytest.fixture(scope="class")
    def dbt_profile_data(
        self, unique_schema, dbt_profile_target, profiles_config_update
    ):
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
        target["root_path"] = unique_schema
        target["database"] = target["datalake"]
        profile["test"]["outputs"]["default"] = target

        if profiles_config_update:
            profile.update(profiles_config_update)
        return profile

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        path = Path(project.project_root) / "test-snapshots"
        Path.mkdir(path)
        write_file(snapshots__test_snapshot, path, "snapshot.sql")

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": properties__test_snapshot_models}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"example_seed.csv": seeds__example_seed_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "snapshot-paths": ["test-snapshots"],
            "models": {},
            "snapshots": {
                "post-hook": [
                    "alter table {{ this }} add columns (new_col int)",
                    "update {{ this }} set new_col = 1",
                ]
            },
            "seeds": {"quote_columns": False, "+twin_strategy": "prevent"},
        }

    def test_hooks_on_snapshots(self, project):
        res = run_dbt(["seed"])
        assert len(res) == 1, "Expected exactly one item"
        res = run_dbt(["snapshot"])
        assert len(res) == 1, "Expected exactly one item"
        res = run_dbt(["test"])
        assert len(res) == 1, "Expected exactly one item"


class PrePostModelHooksInConfigSetup(BaseTestPrePost):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "macro-paths": ["macros"],
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": models__hooks_configured}


class TestPrePostModelHooksInConfig(PrePostModelHooksInConfigSetup):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"+twin_strategy": "prevent"}}

    def test_pre_and_post_model_hooks_model(self, project, dbt_profile_target):
        run_dbt()

        self.check_hooks("start", project, dbt_profile_target.get("host", None))
        self.check_hooks("end", project, dbt_profile_target.get("host", None))


class TestPrePostModelHooksInConfigWithCount(PrePostModelHooksInConfigSetup):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre-hook": [
                        # inside transaction (runs second)
                        MODEL_PRE_HOOK,
                        # outside transaction (runs first)
                        {
                            "sql": "vacuum {{ this.schema }}.on_model_hook",
                            "transaction": False,
                        },
                    ],
                    "post-hook": [
                        # outside transaction (runs second)
                        {
                            "sql": "vacuum {{ this.schema }}.on_model_hook",
                            "transaction": False,
                        },
                        # inside transaction (runs first)
                        MODEL_POST_HOOK,
                    ],
                }
            },
            "seeds": {"+twin_strategy": "prevent"},
        }

    def test_pre_and_post_model_hooks_model_and_project(
        self, project, dbt_profile_target
    ):
        run_dbt()

        self.check_hooks(
            "start", project, dbt_profile_target.get("host", None), count=2
        )
        self.check_hooks("end", project, dbt_profile_target.get("host", None), count=2)


class TestPrePostModelHooksInConfigKwargs(TestPrePostModelHooksInConfig):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"+twin_strategy": "prevent"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": models__hooks_kwargs}


class TestPrePostSnapshotHooksInConfigKwargs(TestPrePostModelHooksOnSnapshots):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        path = Path(project.project_root) / "test-kwargs-snapshots"
        Path.mkdir(path)
        write_file(snapshots__test_snapshot, path, "snapshot.sql")

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "snapshot-paths": ["test-kwargs-snapshots"],
            "models": {},
            "snapshots": {
                "post-hook": [
                    "alter table {{ this }} add columns (new_col int)",
                    "update {{ this }} set new_col = 1",
                ]
            },
            "seeds": {"quote_columns": False, "+twin_strategy": "prevent"},
        }


class TestDuplicateHooksInConfigs(object):
    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": models__hooks_error}

    def test_run_duplicate_hook_defs(self, project):
        with pytest.raises(CompilationError) as exc:
            run_dbt()
        assert "pre_hook" in str(exc.value)
        assert "pre-hook" in str(exc.value)
