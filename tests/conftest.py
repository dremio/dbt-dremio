import os
import pytest
from dotenv import load_dotenv

# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]
load_dotenv()


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        'type': 'dremio',
        'threads': 1,
        'host': os.getenv('DREMIO_HOST'),
        'port': int(os.getenv('DREMIO_PORT')),
        'driver': os.getenv('DREMIO_DRIVER'),
        'user': os.getenv('DREMIO_USERNAME'),
        'password': os.getenv('DREMIO_PASSWORD'),
        'datalake': "rav-test",
        'additional_parameters': 'useEncryption=0;'
    }
