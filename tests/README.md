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
1. Create the source to be tested with and name it dbt_test_source.
1. One of the tests - test_profile_template.py requires creation of three dbt projects in an arbitrary location.
    1. `dbt init test_cloud_options`
        1. Accept all default options
        1. Provide any value for the following options:
            1. user
            1. pat
            1. cloud_project_id
    1. `dbt init test_sw_up_options`
        1. Accept all default options
        1. Provide any value for the following options:
            1. software_host
            1. user
            1. password
    1. `dbt init test_sw_pat_options`
        1. Accept all default options
        1. Provide any value for the following options:
            1. software_host
            1. user
            1. pat

## Executing Tests

In the dbt-dremio directory, run the following command (replacing `<year>`, `<month>`, `<date>`):

`pytest tests/ | tee -a func_test_<year>_<month>_<date>.log`

This will generate the `func_test_<year>_<month>_<date>.log` file. The -a flag prevents overwrites and will just append to an existing file.

If you notice there are specific tests that are failing, you can test them individually using pytest path/to/test.py. Some usual flags to use are:

-   `-r`
    -   Displays a short test summary info which is useful for large test suites.
-   `-v`
    _ Increases verbosity level and shows more information
    _ For even more verbosity, try `-vv` or `-vvv`
    _`--pdb`
    _ Starts PDB - Python’s inbuilt debugger
    _`-x`
    _ Stops the test run after the first failure
