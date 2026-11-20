# Reviewing dbt-dremio

This adapter turns dbt models into Dremio SQL and REST calls. Review for what it does to a user's catalog and data, not for style.

## What failure looks like here

Ranked by cost. Report these first.

1. **Wrong SQL or DDL.** A macro in `dbt/include/dremio/macros/` builds a statement that drops, replaces or overwrites the wrong object. It can also merge, append or snapshot the wrong rows. Check `materializations/incremental/`, `materializations/table/`, `materializations/view/`, `materializations/twin_strategy.sql` and `adapters/snapshot.sql`.
2. **Wrong identifiers or paths.** Quoting or path splitting in `dbt/adapters/dremio/relation.py` or `macros/get_custom_name/` sends a model to the wrong space, source or folder. Watch dots in names, `root_path`/`object_storage_path` and the `datalake` vs `database` split.
3. **Changed materialization behavior.** An existing config (`twin_strategy`, `incremental_strategy`, `format`, reflections, grants, `persist_docs`) does something different than before without a CHANGELOG.md entry.
4. **Broken auth, TLS or credentials.** A change in `dbt/adapters/dremio/credentials.py` or `api/authentication.py` breaks password or PAT login, or `verify_ssl`. A change in `api/rest/` can also log a token or password.
5. **Broken supported versions.** A change breaks dbt-core 1.11, the `dbt-adapters`/`dbt-common` ranges or Python >= 3.10 in `setup.py`. It can also break Dremio Cloud while Software still passes, or the other way round.

## Focus

- `dbt/include/dremio/macros/**`: read the SQL that will actually render, including `is_incremental()` and full-refresh branches.
- `dbt/adapters/dremio/{impl,relation,connections,credentials}.py` and `dbt/adapters/dremio/api/**`.
- `setup.py` and `dev_requirements.txt` version pins.
- Removed or loosened tests in `tests/functional/adapter/` and `tests/unit/`. Check any new entry in `.github/expected_failures*.txt`: it hides a failing test.

## Skip

- `THIRD_PARTY_LICENSES.txt`, `NOTICE`, `LICENSE`, `CODE_OF_CONDUCT.md`
- `.github/scripts/projects.zip` and other binaries
- `.devcontainer/`, `.docker/`, `test.env`, `.pre-commit-config.yaml`, `.bandit.yml`
- `tests/hooks/data/` and other fixture data, unless the PR changes what the test asserts

## Nits

At most 3 per review. Raise them only when nothing above was found, or label each one `nit:`.

## Evidence

Cite `file:line` for every finding and say what the code there does, such as the rendered SQL, the value passed or the branch taken. Do not report a finding inferred from a name, signature, docstring or comment alone. If you could not confirm it, leave it out.

## Summary format

Start with a count line, such as `2 issues to fix, 1 nit`. If there is nothing, write `Nothing to fix.` and stop. Do not praise or recap the PR.
