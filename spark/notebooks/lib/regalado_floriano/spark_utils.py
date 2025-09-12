
from pyspark.sql.types import StructType, IntType, StringType

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        assert(type(columns_info) == dict)
        typeMap = {
            "int": IntType(),
            "string": StringType()
        }
        return SparkUtils.generate_schema(
            (list(  map(
                lambda x: ( ),
                columns_info

            )) )
        )

        raise NotImplementedError("Not implemented yet")
