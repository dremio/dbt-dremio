import pytest
from dbt.tests.util import run_dbt

view1_model = """
SELECT IncidntNum, Category, Descript, DayOfWeek, TO_DATE("SF_incidents2016.json"."Date", 'YYYY-MM-DD', 1) AS "Date", "SF_incidents2016.json"."Time" AS "Time", PdDistrict, Resolution, Address, X, Y, Location, PdId
FROM Samples."samples.dremio.com"."SF_incidents2016.json" AS "SF_incidents2016.json";
"""

view2_model = """
SELECT pickup_datetime, trip_distance_mi FROM Samples."samples.dremio.com"."NYC-taxi-trips" LIMIT 100
"""

reflection1_model = """
{{ config(name='Example 1',
            materialized='reflection',
            display=['Date', 'DayOfWeek', 'PdDistrict', 'Category'], 
            reflection_type='raw')}}
-- depends_on: {{ ref('view1') }}
"""

reflection2_model = """
{{ config(name='Example 2',
            materialized='reflection', 
            reflection_type='aggregation', 
            dimensions=['Date', 'DayOfWeek', 'PdDistrict', 'Category'], 
            measures=['PdId', 'Location'], 
            computations=['COUNT,SUM', 'COUNT'])}}
-- depends_on: {{ ref('view1') }}
"""

reflection3_model = """
{{ config(name='Example 3',
            materialized='reflection',
            display=['Date', 'DayOfWeek', 'PdDistrict', 'Category'], 
            reflection_type='raw',
            partition_by=['DayOfWeek'])}}
-- depends_on: {{ ref('view1') }}
"""

reflection4_model = """
{{ config(
    name='Example 4',
    materialized='reflection', 
    reflection_type='raw', 
    partition_by=['DayOfWeek'],
    partition_method='consolidated'
) }}
-- depends_on: {{ ref('view1') }}
"""

reflection5_model = """
{{ config(
    name='Example 5',
    materialized='reflection', 
    reflection_type='raw', 
    partition_by=['DayOfWeek'],
    partition_method='striped',
    localsort_by=['Date']
) }}
-- depends_on: {{ ref('view1') }}
"""

reflection6_model = """
{{ config(name='Example 6',
            materialized='reflection', 
            reflection_type='aggregate', 
            dimensions=['pickup_datetime'], 
            measures=['trip_distance_mi'],
            date_dimensions=['pickup_datetime'])}}
-- depends_on: {{ ref('view2') }}
"""

class TestReflectionsDremio:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view1.sql": view1_model,
            "view2.sql": view2_model,
            "reflection1.sql": reflection1_model,
            "reflection2.sql": reflection2_model,
            "reflection3.sql": reflection3_model,
            "reflection4.sql": reflection4_model,
            "reflection5.sql": reflection5_model,
            "reflection6.sql": reflection6_model,
        }

    def test_reflection_1(self, project):
        result = run_dbt(["run", "--select", "view1", "reflection1"])
        assert result.status == "success"

        # TODO: implement the following
        # login to dremio and get auth token
        # get the current test folder id
        # get the view (dataset) id
        # get the reflection for the dataset

        assert reflection["name"] == "Example 1"
        assert reflection["type"] == "RAW"
        assert reflection["displayFields"] == [{"name": x} for x in ["Date", "DayOfWeek", "PdDistrict", "Category"]]
        assert "distributionFields" not in reflection
        assert "partitionFields" not in reflection
        assert "sortFields" not in reflection
        assert reflection["partitionDistributionStrategy"] == "STRIPED"

    def test_reflection_2(self, project):
        result = run_dbt_and_capture(["run", "--select", "view1", "reflection2"])
        assert result.status == "success"

        assert reflection["name"] == "Example 2"
        assert reflection["type"] == "AGGREGATION"
        assert reflection["dimensionFields"] == [{"name": x, "granularity": "DATE"} for x in ["Date", "DayOfWeek", "PdDistrict", "Category"]]
        assert reflection["measureFields"] == [{'measureTypeList': ['COUNT', 'SUM'], 'name': 'PdId'},
                                                 {'measureTypeList': ['COUNT'], 'name': 'Location'}]
        assert "distributionFields" not in reflection
        assert "partitionFields" not in reflection
        assert "sortFields" not in reflection
        assert reflection["partitionDistributionStrategy"] == "STRIPED"

    def test_reflection_3(self, project):
        result = run_dbt_and_capture(["run", "--select", "view1", "reflection3"])
        assert result.status == "success"

        assert reflection["name"] == "Example 3"
        assert reflection["type"] == "RAW"
        assert reflection["displayFields"] == [{"name": x} for x in ["Date", "DayOfWeek", "PdDistrict", "Category"]]
        assert reflection["partitionFields"] == [{"name": "DayOfWeek"}]
        assert "distributionFields" not in reflection
        assert "sortFields" not in reflection
        assert reflection["partitionDistributionStrategy"] == "STRIPED"

    def test_reflection_4(self, project):
        result = run_dbt_and_capture(["run", "--select", "view1", "reflection4"])
        assert result.status == "success"

        assert reflection["name"] == "Example 4"
        assert reflection["type"] == "RAW"
        assert reflection["displayFields"] == [{"name": x} for x in ["IncidntNum", "Category", "Descript", "DayOfWeek", "Date", "Time", "PdDistrict", "Resolution", "Address", "X", "Y", "Location", "PdId"]]
        assert reflection["partitionFields"] == [{"name": "DayOfWeek"}]
        assert "distributionFields" not in reflection
        assert "sortFields" not in reflection
        assert reflection["partitionDistributionStrategy"] == "CONSOLIDATED"

    def test_reflection_5(self, project):
        result = run_dbt_and_capture(["run", "--select", "view1", "reflection5"])
        assert result.status == "success"

        assert reflection["name"] == "Example 5"
        assert reflection["type"] == "RAW"
        assert reflection["displayFields"] == [{"name": x} for x in ["IncidntNum", "Category", "Descript", "DayOfWeek", "Date", "Time", "PdDistrict", "Resolution", "Address", "X", "Y", "Location", "PdId"]]
        assert reflection["partitionFields"] == [{"name": "DayOfWeek"}]
        assert reflection["sortFields"] == [{"name": "Date"}]
        assert "distributionFields" not in reflection
        assert reflection["partitionDistributionStrategy"] == "STRIPED"

    def test_reflection_6(self, project):
        result = run_dbt_and_capture(["run", "--select", "view2", "reflection6"])
        assert result.status == "success"

        assert reflection["name"] == "Example 6"
        assert reflection["type"] == "AGGREGATION"
        assert reflection["dimensionFields"] == [{"name": "pickup_datetime", "granularity": "DATE"}]
        assert "measureFields" not in reflection
        assert "distributionFields" not in reflection
        assert "partitionFields" not in reflection
        assert "sortFields" not in reflection
        assert reflection["partitionDistributionStrategy"] == "STRIPED"