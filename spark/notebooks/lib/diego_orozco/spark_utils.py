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
    def parse_type(data_type: str):
        """Permite definir tipos anidados como array<double> o array<array<double>>"""
        data_type = data_type.strip().lower()

        if data_type.startswith("array<") and data_type.endswith(">"):
            inner_type_str = data_type[len("array<"):-1]
            inner_type = SparkUtils.parse_type(inner_type_str)
            return ArrayType(inner_type)

        if data_type in SparkUtils.types_dictionary:
            return SparkUtils.types_dictionary[data_type]

        raise ValueError(f"El tipo de dato no está definido: {data_type}")

    @staticmethod
    def generate_schema(columns_info) -> StructType:
        fields = []
        for col_name, data_type in columns_info:
            fields.append(StructField(col_name, SparkUtils.parse_type(data_type), True))
        return StructType(fields)
