# dbt-dremio 1.3.1 - release tbd

## Features

## Fixes

-   Override dbt-core `default__type_string()` macro to use Dremio Supported VARCHAR instead of the default string. ([#80](https://github.com/dremio/dbt-dremio/pull/80))

-   Change \_populate_job_results() to have an optional row_limit argument with default set to 100 (Dremio's default). ([#61](https://github.com/dremio/dbt-dremio/issues/61))

-   Implement pagination in \_populate_job_results() ([#61](https://github.com/dremio/dbt-dremio/issues/61))

-   Fix error handling so the error reported when a job fails is the actual error from Dremio. ([#69](https://github.com/dremio/dbt-dremio/issues/69))

## Under the Hood
* [#32](https://github.com/dremio/dbt-dremio/issues/32) Add pre-commit hooks (most significant being `black`, `flake8`, and `bandit`)

## Dependency
