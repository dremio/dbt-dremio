# Tests

Tests are written using [pytest](https://docs.pytest.org/), with some mocking being done with [unittest](https://docs.python.org/3/library/unittest.mock.html).

## Setup

To run our tests, a test environment must be set up.

1. Install dev requirements using `pip install -r dev_requirements.txt` from the dbt-dremio directory.
1. First, create a tests/.env file with the following content. Note you will have to fill in the Dremio hostname, username and password. For software version of Dremio:
    ```
    DREMIO_SOFTWARE_HOST=
    DREMIO_SOFTWARE_USERNAME=
    DREMIO_SOFTWARE_PASSWORD=
    DREMIO_DATALAKE=dbt_test_source
    DREMIO_DATABASE=dbt_test
    DBT_TEST_USER_1=dbt_test_user_1
    DBT_TEST_USER_2=dbt_test_user_2
    DBT_TEST_USER_3=dbt_test_user_3
    DBT_TEST_ROLE_1=dbt_test_role_1
    DBT_TEST_ROLE_2=dbt_test_role_2
    ```
   For cloud version of Dremio:
    ```
    DREMIO_CLOUD_HOST=
    DREMIO_CLOUD_PROJECT_ID=
    DREMIO_CLOUD_USERNAME=
    DREMIO_PAT=
    DREMIO_DATALAKE=dbt_test_source
    DREMIO_DATABASE=dbt_test
    DBT_TEST_USER_1=dbt_test_user_1
    DBT_TEST_USER_2=dbt_test_user_2
    DBT_TEST_USER_3=dbt_test_user_3
    DBT_TEST_ROLE_1=dbt_test_role_1
    DBT_TEST_ROLE_2=dbt_test_role_2
    ```
1. Create the three users listed above (dbt_test_user_1, dbt_test_user_2, dbt_test_user_3) in the Dremio instance.
1. Create a bucket in Object storage with a name `dbtdremios3`
1. Create an object storage source in Dremio called `dbt_test_source`.
    1. An example would be a [gcs object storage source](https://docs.dremio.com/software/data-sources/gcs/).
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
    -   Increases verbosity level and shows more information
    -   For even more verbosity, try `-vv` or `-vvv`
-   `--pdb`
    -   Starts PDB - Python’s inbuilt debugger
-   `-x`
    -   Stops the test run after the first failure
