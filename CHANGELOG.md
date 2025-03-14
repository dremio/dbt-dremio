# dbt-dremio v1.8.2

## Changes

- When naming reflections, if a `name` config is not set, the `alias` config parameter will be used instead. If also undefined, it will refer to the model name instead of using `Unnamed Reflection`
- Grants can now be set for both users and for roles. A prefix was added to handle this, with `user:` and `role:` being the valid prefixes. For example, `user:dbt_test_user_1` and `role:dbt_test_role_1`. If no prefix is provided, defaults to user for backwards compatibility.
- Moves the `raw_on_schema_change` variable back into scope for the config validator
- Adds `BaseIncrementalOnSchemaChange` test to test_incremental.py
- Changed logic for partitioning when materializing tables. Double quoting issue has been removed, now letting the user decide the quoting
    - New example: `partition_by=['month("datetime_utc")']`
- Fixed a bug with grabbing the column schemas from queries (names and data types)
    - Invisible to the users but it was the cause for some other bugs users were facing that have now been fixed
## Features

- [#259](https://github.com/dremio/dbt-dremio/pull/259) Added support for roles in grants
- [#273](https://github.com/dremio/dbt-dremio/pull/273) Fix issue with on_schema_change config
- [#282](https://github.com/dremio/dbt-dremio/pull/282) Fix issue with get_column_schema_from_query

# dbt-dremio v1.8.1

## Changes

-   Added [DremioRestClient](dbt/adapters/dremio/api/rest/client.py) to isolate all Dremio API calls inside one class
- [#256](https://github.com/dremio/dbt-dremio/pull/256) Reflections are now handled through the Rest API
  - Non-admin users are now able to use reflections
  - It is now possible to set a custom name for reflections
  - If a reflection already exists in the dataset with the same name defined in the model, it will be updated instead of creating a new one
  - New `date_dimensions` parameter was added to the reflection materialization, to set fields that have a `DATE` granularity
  - Added Distribution Fields under `distribute_by`
  - Added partition transformations under `partition_transform`
    - Defaults to Original/Identity if not defined
    - `year/month/day/hour/bucket(n)/truncate(n)`
  - Computations default to `SUM, COUNT` if mapped measure is numeric, `COUNT` if not
  - `reflections_enabled` adapter option has been renamed to `reflections_metadata_enabled` (requires user privileges to run in dremio)
-   Removing duplicated macros array_append, array_concat as Dremio already has SQL functions analogues.
- [#250](https://github.com/dremio/dbt-dremio/pull/250) Possibility to integrate wikis and tags by enabling `relation` option from `persist_docs` configuration
  - New macro `dremio__persist_docs` created
  - Views also perform `persist_docs` macro
  - Integration via REST API

## Features

-   [#250](https://github.com/dremio/dbt-dremio/pull/250) Implementation of wikis and tags feature
-   [#250](https://github.com/dremio/dbt-dremio/pull/256) Reflections are now handled through the Rest API

# dbt-dremio v1.8.0

## Dependency

-   [#222](https://github.com/dremio/dbt-dremio/issues/222) Upgrade dbt-core to 1.8.8 and dbt-tests-adapter to 1.8.0

## Features

-   [#223](https://github.com/dremio/dbt-dremio/issues/224) Implement merge strategy for incremental materializations
-   [#229](https://github.com/dremio/dbt-dremio/issues/229) Add max operator to get_relation_last_modified macro

# dbt-dremio v1.7.0

## Changes

-   [#195](https://github.com/dremio/dbt-dremio/issues/195) Ensure the adapter does not try and create folders in object storage source
-   [#220](https://github.com/dremio/dbt-dremio/pull/220) Optimize networking performance with Dremio server


# dbt-dremio v1.5.1

## Changes

-   [#199](https://github.com/dremio/dbt-dremio/issues/199) Populate PyPI's `long_description` with contents of `README.md`
-   [#167](https://github.com/dremio/dbt-dremio/issues/167) Remove parentheses surrounding views in the create_view_as macro. In more complex queries, the parentheses cause performance issues.
-   [#211](https://github.com/dremio/dbt-dremio/issues/211) Make fetching model data false by default. This improves performance where job results do not need to be populated.
-   [#203](https://github.com/dremio/dbt-dremio/issues/203) Allow for dots in schema name, by surrounding in single and double quotes.
-   [#193](https://github.com/dremio/dbt-dremio/issues/193) Fixes Reflection bug: The name argument to ref() must be a string, got <class 'jinja2.runtime.Undefined'>
-   [Versioning](https://github.com/dremio/dbt-dremio/pull/210) Added optional parameter v to the ref macro

# dbt-dremio 1.5.0 - release June 22, 2023


## Features

## Fixes

## Under the Hood
-   [#179](https://github.com/dremio/dbt-dremio/issues/179) Upgrade to support dbt-core v1.5.0.
    -   Add support for Python 3.11.
    -   Add support for relevant Tests:
        -   caching
        -   hooks
        -   simple_copy
-   Add support for model contracts (Stub the feature to let users know the feature is not supported).

## Dependency

-   Upgrade sqlparse to 0.4.4 [#180](https://github.com/dremio/dbt-dremio/issues/180).
-   Upgrade dbt-core to 1.5.0.
-   Upgrade dbt-tests-adapter to 1.5.0.
-   Upgrade Requests to 2.31.0. [#183](https://github.com/dremio/dbt-dremio/issues/183).


# dbt-dremio 1.4.5 - release March 23, 2023

## Features

## Fixes

-   [#142](https://github.com/dremio/dbt-dremio/issues/142) Ensure ssl verification is enabled in all api calls. Also added an option called `verify_ssl` so it can be disabled in necessary circumstances.

## Under the Hood

-   [#64](https://github.com/dremio/dbt-dremio/issues/64) Add BaseArrayTests and throw exceptions for unsupported Array Macros.
-   [#117](https://github.com/dremio/dbt-dremio/issues/117) Add support for Query Comment Tests and Python 3.11
-   [#134](https://github.com/dremio/dbt-dremio/issues/134) Add dremio:exact_search_enabled variable that if set to true, replaces usage of ilike with a basic equality in dremio\_\_list_relations_without_caching when reflections are not enabled.
-   [#117](https://github.com/dremio/dbt-dremio/issues/117) Add Base Current Timestamps Tests
-   [#117](https://github.com/dremio/dbt-dremio/issues/117) Replace deprecated dbt-core exceptions
-   [#117](https://github.com/dremio/dbt-dremio/issues/117) Add support for changing relation type test

## Dependency

-   Upgrade dbt-core to 1.4.5.
-   Upgrade dbt-tests-adapter to 1.4.5.

# dbt-dremio 1.3.2 - release February 8, 2023

## Features

## Fixes

-   Override dbt-core `default__type_string()` macro to use Dremio Supported VARCHAR instead of the default string. ([#80](https://github.com/dremio/dbt-dremio/pull/80))

-   Change \_populate_job_results() to have an optional row_limit argument with default set to 100 (Dremio's default). ([#61](https://github.com/dremio/dbt-dremio/issues/61))

-   Implement pagination in \_populate_job_results() ([#61](https://github.com/dremio/dbt-dremio/issues/61))

-   Fix error handling so the error reported when a job fails is the actual error from Dremio. ([#69](https://github.com/dremio/dbt-dremio/issues/69))

-   Override dbt-core `default__rename_relation()` macro to use Dremio Supported CTAS and DROP instead of ALTER TABLE and RENAME to. ([#44](https://github.com/dremio/dbt-dremio/issues/44))

## Under the Hood

-   [#32](https://github.com/dremio/dbt-dremio/issues/32) Add pre-commit hooks (most significant being `black`, `flake8`, and `bandit`)

-   Implement new Incremental materialization logic from dbt 1.3 as part of the upgrade to support dbt-core v1.3.0. ([#44](https://github.com/dremio/dbt-dremio/issues/44), [#16](https://github.com/dremio/dbt-dremio/issues/16))

## Dependency

-   Upgrade dbt-core to 1.3.2.

-   Upgrade dbt-tests-adapter to 1.3.2.
