# Tests

Tests are written using [pytest](https://docs.pytest.org/), with some mocking being done with [unittest](https://docs.python.org/3/library/unittest.mock.html).

## Setup

To run our tests, a test environment must be set up.

1. First, create a tests/.env file with the following content. Note you will have to fill in the Dremio hostname, username and password.
    ```
    DREMIO_HOST=
    DREMIO_USERNAME=
    DREMIO_PASSWORD=
    DREMIO_DATALAKE=dbt_test_source
    DREMIO_DATABASE=dbt_test
    DBT_TEST_USER_1=dbt_test_user_1
    DBT_TEST_USER_2=dbt_test_user_2
    DBT_TEST_USER_3=dbt_test_user_3
    ```
1. Create the three users listed above (dbt_test_user_1, dbt_test_user_2, dbt_test_user_3) in the Dremio instance.
1. Create the source to be tested with.
