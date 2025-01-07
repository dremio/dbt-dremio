from enum import Enum


class TransformType(Enum):
    YEAR = "YEAR"
    MONTH = "MONTH"
    DAY = "DAY"
    HOUR = "HOUR"
    IDENTITY = "IDENTITY"
    BUCKET = "BUCKET"
    TRUNCATE = "TRUNCATE"

    @classmethod
    def from_string(cls, transform_str):
        transform_str = transform_str.upper()

        if transform_str.startswith("BUCKET("):
            return cls.BUCKET
        elif transform_str.startswith("TRUNCATE("):
            return cls.TRUNCATE

        try:
            return cls(transform_str)
        except ValueError:
            return cls.IDENTITY

    def to_transform(self, raw_str):
        if self in (
                TransformType.YEAR,
                TransformType.MONTH,
                TransformType.DAY,
                TransformType.HOUR,
                TransformType.IDENTITY
        ):
            return {"type": self.value}

        if self == TransformType.BUCKET:
            bucket_count = int(raw_str.split("(")[1].split(")")[0])
            return {
                "type": "BUCKET",
                "bucketTransform": {"bucketCount": bucket_count},
            }

        if self == TransformType.TRUNCATE:
            truncate_length = int(raw_str.split("(")[1].split(")")[0])
            return {
                "type": "TRUNCATE",
                "truncateTransform": {"truncateLength": truncate_length},
            }

        return {"type": TransformType.IDENTITY.value}


# https://docs.dremio.com/current/reference/api/reflections/
class ReflectionEntity:
    def __init__(self, name, reflection_type, dataset_id, display_fields, dimensions, date_dimensions, measures,
                 computations, partition_by, partition_transform, partition_method, distribute_by, localsort_by,
                 arrow_cache):
        self.__name = name
        self.__type = reflection_type
        self.__dataset_id = dataset_id
        self.__partition_method = partition_method.upper()
        self.__display_fields = display_fields
        self.__dimensions_fields = dimensions
        self.__date_dimensions_fields = date_dimensions
        self.__measure_fields = measures
        self.__computation_fields = computations
        self.__partition_by_fields = partition_by
        self.__partition_transformations = partition_transform
        self.__partition_method = partition_method
        self.__distribute_by_fields = distribute_by
        self.__local_sort_fields = localsort_by
        self.__arrow_cache = arrow_cache

    def buildDisplayFields(self):
        return [{"name": field} for field in self.__display_fields]

    def buildDimensionFields(self):
        return [{"name": field} for field in self.__dimensions_fields]

    def buildDateFields(self):
        return [{"name": date_dimension, "granularity": "DATE"} for date_dimension in self.__date_dimensions_fields]

    def buildMeasureFields(self):
        return [{"name": measure, "measureTypeList": computation.split(',')} for
                measure, computation in zip(self.__measure_fields, self.__computation_fields)]

    def buildPartitionFields(self):
        if not self.__partition_transformations:
            self.__partition_transformations = ["IDENTITY"] * len(self.__partition_by_fields)

        partition_fields = []

        for partition, transform in zip(self.__partition_by_fields, self.__partition_transformations):
            transform_type = TransformType.from_string(transform)
            partition_fields.append({
                "name": partition,
                "transform": transform_type.to_transform(transform)
            })

        return partition_fields

    def buildDistributionFields(self):
        return [{"name": distribute} for distribute in self.__distribute_by_fields]

    def buildSortFields(self):
        return [{"name": sort} for sort in self.__local_sort_fields]

    def build_payload(self):
        payload = {
            "type": self.__type,
            "name": self.__name,
            "datasetId": self.__dataset_id,
            "enabled": True,
            "arrowCachingEnabled": self.__arrow_cache,
            "partitionDistributionStrategy": self.__partition_method.upper(),
            "entityType": "reflection"
        }

        if self.__display_fields:
            payload["displayFields"] = self.buildDisplayFields()

        if self.__dimensions_fields:
            payload["dimensionFields"] = self.buildDimensionFields()

        if self.__date_dimensions_fields:
            payload["dateFields"] = self.buildDateFields()

        if self.__measure_fields and self.__computation_fields:
            payload["measureFields"] = self.buildMeasureFields()

        if self.__partition_by_fields:
            payload["partitionFields"] = self.buildPartitionFields()

        if self.__distribute_by_fields:
            payload["distributionFields"] = self.buildDistributionFields()

        if self.__local_sort_fields:
            payload["sortFields"] = self.buildSortFields()

        return payload
