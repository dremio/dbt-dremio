# dbt-dremio 1.3.1 - release tbd

## Features

## Fixes

-   Override dbt-core `default__type_string()` macro to use Dremio Supported VARCHAR instead of the default string. ([#80](https://github.com/dremio/dbt-dremio/pull/80))

-   Change \_populate_job_results() to have page_limit of 100 (Dremio's default).

-   Change \_populate_job_results() to have an additional optional page_limit argument.

## Under the Hood

## Dependency
