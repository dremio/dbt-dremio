import pytest
from build.lib.dbt.adapters.dremio.api.rest.client import DremioRestClient
from dbt.adapters.dremio import DremioCredentials
from dbt.adapters.dremio.api.parameters import ParametersBuilder
from dbt.adapters.dremio.api.authentication import DremioAuthentication
from dbt.tests.util import run_dbt, run_dbt_and_capture
from pydantic.experimental.pipeline import transform


view1_model = """
SELECT IncidntNum, Category, Descript, DayOfWeek, TO_DATE("SF_incidents2016.json"."Date", 'YYYY-MM-DD', 1) AS "Date", "SF_incidents2016.json"."Time" AS "Time", PdDistrict, Resolution, Address, X, Y, Location, PdId
FROM Samples."samples.dremio.com"."SF_incidents2016.json" AS "SF_incidents2016.json";
"""

view2_model = """
SELECT pickup_datetime, trip_distance_mi FROM Samples."samples.dremio.com"."NYC-taxi-trips" LIMIT 100
"""

basic_raw_model = """
{{ config(name='Basic Raw Reflection',
            materialized='reflection',
            display=['Date', 'DayOfWeek', 'PdDistrict', 'Category'], 
            reflection_type='raw')}}
-- depends_on: {{ ref('view1') }}
"""

basic_aggregation_model = """
{{ config(name='Basic Aggregation Reflection',
            materialized='reflection', 
            reflection_type='aggregation', 
            dimensions=['Date', 'DayOfWeek', 'PdDistrict', 'Category'], 
            measures=['PdId', 'Location'], 
            computations=['COUNT,SUM', 'COUNT'])}}
-- depends_on: {{ ref('view1') }}
"""

partition_by_model = """
{{ config(name='Partition By Reflection',
            materialized='reflection',
            display=['Date', 'DayOfWeek', 'PdDistrict', 'Category'], 
            reflection_type='raw',
            partition_by=['DayOfWeek'])}}
-- depends_on: {{ ref('view1') }}
"""

consolidated_partition_method_model = """
{{ config(
    name='Consolidated Partition Method Reflection',
    materialized='reflection', 
    display=['Date', 'DayOfWeek', 'PdDistrict', 'Category'],
    reflection_type='raw', 
    partition_by=['DayOfWeek'],
    partition_method='consolidated'
) }}
-- depends_on: {{ ref('view1') }}
"""

localsort_by_model = """
{{ config(
    name='Localsort By Reflection',
    materialized='reflection',
    display=['Date', 'DayOfWeek', 'PdDistrict', 'Category'], 
    reflection_type='raw', 
    partition_by=['DayOfWeek'],
    partition_method='striped',
    localsort_by=['Date']
) }}
-- depends_on: {{ ref('view1') }}
"""

granular_date_dimension_model = """
{{ config(name='Granulate Date Dimension Reflection',
            materialized='reflection', 
            reflection_type='aggregate', 
            dimensions=['pickup_datetime'], 
            measures=['trip_distance_mi'],
            date_dimensions=['pickup_datetime'])}}
-- depends_on: {{ ref('view2') }}
"""

distribute_by_model = """
{{ config(
    name='Distribute By Reflection',
    materialized='reflection',
    reflection_type='aggregate',
    dimensions=['Date', 'DayOfWeek', 'PdDistrict', 'Category'],
    distribute_by=['DayOfWeek']
) }}
-- depends_on: {{ ref('view1') }}
"""

transformations_model = """
{{ config(
    name='Transformations Reflection',
    materialized='reflection',
    reflection_type='aggregate',
    dimensions=['Date', 'DayOfWeek', 'PdDistrict', 'Category'],
    partition_by=['Date', 'DayOfWeek', 'PdDistrict', 'Category'],
    partition_transform=['YEAR', 'TRUNCATE(2)', 'BUCKET(5)', 'IDENTITY'],
) }}
-- depends_on: {{ ref('view1') }}
"""

default_transformations_model = """
{{ config(
    name='Default Transformations Reflection',
    materialized='reflection',
    reflection_type='aggregate',
    dimensions=['Date', 'DayOfWeek', 'PdDistrict', 'Category'],
    partition_by=['Date', 'DayOfWeek', 'PdDistrict', 'Category']
) }}
-- depends_on: {{ ref('view1') }}
"""

default_computations_model = """
{{ config(name='Default Computations Reflection',
            materialized='reflection',
            reflection_type='aggregate',
            measures=['pickup_datetime', 'trip_distance_mi'])}}
-- depends_on: {{ ref('view2') }}
"""

default_displays_model = """
{{ config(name='Default Displays Reflection',
            materialized='reflection',
            reflection_type='raw')}}
-- depends_on: {{ ref('view1') }}
"""

name_reflection_from_alias_model = """
{{ config(alias='Reflection name from alias',
            materialized='reflection',
            display=['Date', 'DayOfWeek', 'PdDistrict', 'Category'],
            reflection_type='raw')}}
-- depends_on: {{ ref('view1') }}
"""

name_reflection_from_filename_model = """
{{ config(materialized='reflection',
            display=['Date', 'DayOfWeek', 'PdDistrict', 'Category'],
            reflection_type='raw')}}
-- depends_on: {{ ref('view1') }}
"""

wait_strategy_timeout_reflection = """
{{ config(alias='This will mock a timeout when using wait',
            materialized='reflection',
            reflection_strategy='wait',
            max_wait_time=1,
            display=['Date', 'DayOfWeek', 'PdDistrict', 'Category'],
            reflection_type='raw')}}
-- depends_on: {{ ref('view1') }}
"""

trigger_strategy_timeout_reflection = """
{{ config(alias='This will mock a timeout when using trigger',
            materialized='reflection',
            reflection_strategy='trigger',
            max_wait_time=1,
            display=['Date', 'DayOfWeek', 'PdDistrict', 'Category'],
            reflection_type='raw')}}
-- depends_on: {{ ref('view1') }}
"""

class TestReflectionsDremio:
    @pytest.fixture(scope="class")
    def client(self, adapter):
        credentials = adapter.connections.profile.credentials
        parameters = ParametersBuilder.build(credentials)
        client = DremioRestClient(parameters.get_parameters())
        client.start()

        return client

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view1.sql": view1_model,
            "view2.sql": view2_model,
            "basic_raw.sql": basic_raw_model,
            "basic_aggregation.sql": basic_aggregation_model,
            "partition_by.sql": partition_by_model,
            "consolidated_partition_method.sql": consolidated_partition_method_model,
            "localsort_by.sql": localsort_by_model,
            "granular_date_dimension.sql": granular_date_dimension_model,
            "distribute_by.sql": distribute_by_model,
            "transformations.sql": transformations_model,
            "default_transformations.sql": default_transformations_model,
            "default_computations.sql": default_computations_model,
            "default_displays_model.sql": default_displays_model,
            "name_reflection_from_alias.sql": name_reflection_from_alias_model,
            "name_reflection_from_filename.sql": name_reflection_from_filename_model,
            "wait_strategy_timeout_reflection.sql": wait_strategy_timeout_reflection,
            "trigger_strategy_timeout_reflection.sql": trigger_strategy_timeout_reflection,
        }

    def _create_path_list(self, database, schema):
        path = [database]
        if schema != 'no_schema':
            folders = schema.split(".")
            path.extend(folders)
        return path

    def _get_reflection(self, project, client, identifier, expected_name):
        database = project.database
        schema = project.test_schema
        path = self._create_path_list(database, schema)

        path.append(identifier)

        catalog_info = client.get_catalog_item(
            catalog_id=None,
            catalog_path=path,
        )

        reflections = client.get_reflections(catalog_info['id'])['data']

        reflection = next((x for x in reflections if x['name'] == expected_name), None)

        return reflection

    def testBasicRawReflection(self, project, client):
        run_dbt(["run", "--select", "view1", "basic_raw"])

        reflection = self._get_reflection(project, client, "view1", "Basic Raw Reflection")

        assert reflection
        assert reflection["name"] == "Basic Raw Reflection"
        assert reflection["type"] == "RAW"
        assert reflection["displayFields"] == [{"name": x} for x in ["Date", "DayOfWeek", "PdDistrict", "Category"]]
        assert "dimensionFields" not in reflection
        assert "measureFields" not in reflection
        assert "distributionFields" not in reflection
        assert "partitionFields" not in reflection
        assert "sortFields" not in reflection
        assert reflection["partitionDistributionStrategy"] == "STRIPED"

    def testBasicAggregationReflection(self, project, client):
        run_dbt(["run", "--select", "view1", "basic_aggregation"])

        reflection = self._get_reflection(project, client, "view1", "Basic Aggregation Reflection")

        assert reflection["name"] == "Basic Aggregation Reflection"
        assert reflection["type"] == "AGGREGATION"
        assert reflection["dimensionFields"] == [{"name": x, "granularity": "DATE"} for x in
                                                 ["Date", "DayOfWeek", "PdDistrict", "Category"]]
        assert reflection["measureFields"] == [
            {"name": "PdId", "measureTypeList": ["COUNT", "SUM"]},
            {"name": "Location", "measureTypeList": ["COUNT"]}
        ]
        assert "distributionFields" not in reflection
        assert "partitionFields" not in reflection
        assert "sortFields" not in reflection
        assert reflection["partitionDistributionStrategy"] == "STRIPED"

    def testPartitionByReflection(self, project, client):
        run_dbt(["run", "--select", "view1", "partition_by"])

        reflection = self._get_reflection(project, client, "view1", "Partition By Reflection")

        assert reflection["name"] == "Partition By Reflection"
        assert reflection["type"] == "RAW"
        assert reflection["displayFields"] == [{"name": x} for x in ["Date", "DayOfWeek", "PdDistrict", "Category"]]
        assert reflection["partitionFields"] == [{"name": "DayOfWeek", "transform": {"type": "IDENTITY"}}]
        assert "dimensionFields" not in reflection
        assert "measureFields" not in reflection
        assert "distributionFields" not in reflection
        assert "sortFields" not in reflection
        assert reflection["partitionDistributionStrategy"] == "STRIPED"

    def testConsolidatedPartitionMethodReflection(self, project, client):
        run_dbt(["run", "--select", "view1", "consolidated_partition_method"])

        reflection = self._get_reflection(project, client, "view1", "Consolidated Partition Method Reflection")

        assert reflection["name"] == "Consolidated Partition Method Reflection"
        assert reflection["type"] == "RAW"
        assert reflection["displayFields"] == [{"name": x} for x in ["Date", "DayOfWeek", "PdDistrict", "Category"]]
        assert reflection["partitionFields"] == [{"name": "DayOfWeek", "transform": {"type": "IDENTITY"}}]
        assert "dimensionFields" not in reflection
        assert "measureFields" not in reflection
        assert "distributionFields" not in reflection
        assert "sortFields" not in reflection
        assert reflection["partitionDistributionStrategy"] == "CONSOLIDATED"

    def testLocalsortByReflection(self, project, client):
        run_dbt(["run", "--select", "view1", "localsort_by"])

        reflection = self._get_reflection(project, client, "view1", "Localsort By Reflection")

        assert reflection["name"] == "Localsort By Reflection"
        assert reflection["type"] == "RAW"
        assert reflection["displayFields"] == [{"name": x} for x in ["Date", "DayOfWeek", "PdDistrict", "Category"]]
        assert reflection["partitionFields"] == [{"name": "DayOfWeek", "transform": {"type": "IDENTITY"}}]
        assert "dimensionFields" not in reflection
        assert "measureFields" not in reflection
        assert "distributionFields" not in reflection
        assert reflection["sortFields"] == [{"name": "Date"}]
        assert reflection["partitionDistributionStrategy"] == "STRIPED"

    def testGranularDateDimensionReflection(self, project, client):
        run_dbt(["run", "--select", "view2", "granular_date_dimension"])

        reflection = self._get_reflection(project, client, "view2", "Granulate Date Dimension Reflection")

        assert reflection["name"] == "Granulate Date Dimension Reflection"
        assert reflection["type"] == "AGGREGATION"
        assert reflection["dimensionFields"] == [{"name": "pickup_datetime", "granularity": "DATE"}]
        assert reflection["measureFields"] == [{"name": "trip_distance_mi", "measureTypeList": ["SUM", "COUNT"]}]
        assert "distributionFields" not in reflection
        assert "partitionFields" not in reflection
        assert "sortFields" not in reflection
        assert reflection["partitionDistributionStrategy"] == "STRIPED"

    def testDistributeByReflection(self, project, client):
        run_dbt(["run", "--select", "view1", "distribute_by"])

        reflection = self._get_reflection(project, client, "view1", "Distribute By Reflection")

        assert reflection["name"] == "Distribute By Reflection"
        assert reflection["type"] == "AGGREGATION"
        assert reflection["dimensionFields"] == [{"name": x, "granularity": "DATE"} for x in
                                                 ["Date", "DayOfWeek", "PdDistrict", "Category"]]
        assert "measureFields" not in reflection
        assert reflection["distributionFields"] == [{"name": "DayOfWeek"}]
        assert "partitionFields" not in reflection
        assert "sortFields" not in reflection
        assert reflection["partitionDistributionStrategy"] == "STRIPED"

    def testTransformationsReflection(self, project, client):
        run_dbt(["run", "--select", "view1", "transformations"])

        reflection = self._get_reflection(project, client, "view1", "Transformations Reflection")

        assert reflection["name"] == "Transformations Reflection"
        assert reflection["type"] == "AGGREGATION"
        assert reflection["dimensionFields"] == [{"name": x, "granularity": "DATE"} for x in
                                                 ["Date", "DayOfWeek", "PdDistrict", "Category"]]
        assert "measureFields" not in reflection
        assert "distributionFields" not in reflection
        assert reflection["partitionFields"] == [
            {"name": "Date", "transform": {"type": "YEAR"}},
            {"name": "DayOfWeek", "transform": {"type": "TRUNCATE", "truncateTransform": {"truncateLength": 2}}},
            {"name": "PdDistrict", "transform": {"type": "BUCKET", "bucketTransform": {"bucketCount": 5}}},
            {"name": "Category", "transform": {"type": "IDENTITY"}}
        ]
        assert "sortFields" not in reflection
        assert reflection["partitionDistributionStrategy"] == "STRIPED"

    def testDefaultTransformationsReflection(self, project, client):
        run_dbt(["run", "--select", "view1", "default_transformations"])

        reflection = self._get_reflection(project, client, "view1", "Default Transformations Reflection")

        assert reflection["name"] == "Default Transformations Reflection"
        assert reflection["type"] == "AGGREGATION"
        assert reflection["dimensionFields"] == [{"name": x, "granularity": "DATE"} for x in
                                                 ["Date", "DayOfWeek", "PdDistrict", "Category"]]
        assert "measureFields" not in reflection
        assert "distributionFields" not in reflection
        assert reflection["partitionFields"] == [
            {"name": x, "transform": {"type": "IDENTITY"}} for x in ["Date", "DayOfWeek", "PdDistrict", "Category"]
        ]
        assert "sortFields" not in reflection
        assert reflection["partitionDistributionStrategy"] == "STRIPED"

    def testDefaultComputationsReflection(self, project, client):
        run_dbt(["run", "--select", "view2", "default_computations"])

        reflection = self._get_reflection(project, client, "view2", "Default Computations Reflection")

        assert reflection["name"] == "Default Computations Reflection"
        assert reflection["type"] == "AGGREGATION"
        assert reflection["dimensionFields"] == [{"name": "pickup_datetime", "granularity": "DATE"}]
        assert reflection["measureFields"] == [
            {"name": "pickup_datetime", "measureTypeList": ["COUNT"]},
            {"name": "trip_distance_mi", "measureTypeList": ["SUM", "COUNT"]}
        ]
        assert "distributionFields" not in reflection
        assert "partitionFields" not in reflection
        assert "sortFields" not in reflection
        assert reflection["partitionDistributionStrategy"] == "STRIPED"

    def testDefaultDisplaysReflection(self, project, client):
        run_dbt(["run", "--select", "view1", "default_displays_model"])

        reflection = self._get_reflection(project, client, "view1", "Default Displays Reflection")

        assert reflection["name"] == "Default Displays Reflection"
        assert reflection["type"] == "RAW"
        assert reflection["displayFields"] == [{"name": x} for x in
                                               ["IncidntNum", "Category", "Descript", "DayOfWeek", "Date", "Time",
                                                "PdDistrict", "Resolution", "Address", "X", "Y", "Location", "PdId"]]
        assert "dimensionFields" not in reflection
        assert "measureFields" not in reflection
        assert "distributionFields" not in reflection
        assert "partitionFields" not in reflection
        assert "sortFields" not in reflection
        assert reflection["partitionDistributionStrategy"] == "STRIPED"

    def testNameReflectionFromAlias(self, project, client):
        run_dbt(["run", "--select", "view1", "name_reflection_from_alias"])

        reflection = self._get_reflection(project, client, "view1", "Reflection name from alias")

        assert reflection
        assert reflection["name"] == "Reflection name from alias"
        assert reflection["type"] == "RAW"
        assert reflection["displayFields"] == [{"name": x} for x in ["Date", "DayOfWeek", "PdDistrict", "Category"]]
        assert "dimensionFields" not in reflection
        assert "measureFields" not in reflection
        assert "distributionFields" not in reflection
        assert "partitionFields" not in reflection
        assert "sortFields" not in reflection
        assert reflection["partitionDistributionStrategy"] == "STRIPED"

    def testNameReflectionFromFilename(self, project, client):
        run_dbt(["run", "--select", "view1", "name_reflection_from_filename"])

        reflection = self._get_reflection(project, client, "view1", "name_reflection_from_filename")

        assert reflection
        assert reflection["name"] == "name_reflection_from_filename"
        assert reflection["type"] == "RAW"
        assert reflection["displayFields"] == [{"name": x} for x in ["Date", "DayOfWeek", "PdDistrict", "Category"]]
        assert "dimensionFields" not in reflection
        assert "measureFields" not in reflection
        assert "distributionFields" not in reflection
        assert "partitionFields" not in reflection
        assert "sortFields" not in reflection
        assert reflection["partitionDistributionStrategy"] == "STRIPED"

    def testWaitStrategyTimeoutReflection(self, project):
        (results, log_output) = run_dbt_and_capture(["run", "--select", "view1", "wait_strategy_timeout_reflection"])
        assert "did not become available within 1 seconds, skipping wait" in log_output

    def testTriggerStrategyTimeoutReflection(self, project):
        (results, log_output) = run_dbt_and_capture(["run", "--select", "view1", "trigger_strategy_timeout_reflection"])
        assert "did not become available within 1 seconds, skipping wait" not in log_output
