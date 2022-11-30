# dbt-dremio 1.3.1 - release tbd

## Features

## Fixes

-   Override dbt-core `default__type_string()` macro to use Dremio Supported VARCHAR instead of the default string. ([#80](https://github.com/dremio/dbt-dremio/pull/80))

-   Change \_populate_job_results() to have limit of 500 (Dremio's limit).

## Under the Hood
* [#32](https://github.com/dremio/dbt-dremio/issues/32) Add pre-commit hooks (most significant being `black`, `flake8`, and `bandit`)

## Dependency
