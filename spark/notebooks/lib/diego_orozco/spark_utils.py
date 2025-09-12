from urllib.parse import urlparse
from pyspark.sql.types import StructType, StructField ,StringType, IntegerType, IntegerType, ShortType, DoubleType, FloatType, BooleanType, DateType, FloatType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, MapType

class SparkUtils:
    # Dictionary with supported types
    types_dictionary = {
        "string": StringType(),
        "int": IntegerType(),
        "short": ShortType(),
        "double": DoubleType(),
        "float": FloatType(),
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "binary": BinaryType()
    }

    @staticmethod
    def generate_schema(columns_info) -> StructType:
        data = []
        for col_name, data_type in columns_info:
            if data_type not in SparkUtils.types_dictionary:
                raise ValueError(f"El tipo de dato no esta definido: {data_type}")
            data.append(StructField(col_name, SparkUtils.types_dictionary[data_type], True))
        return StructType(data)
