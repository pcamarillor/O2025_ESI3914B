from urllib.parse import urlparse
from pyspark.sql.types import StructType, StringType, IntegerType, IntegerType, ShortType, DoubleType, FloatType, BooleanType, DateType, FloatType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, MapType, StructField

class SparkUtils:
    types = {
        "string": StringType(),
        "int": IntegerType(),
        "short": ShortType(),
        "double": DoubleType(),
        "float": FloatType(),
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "binary": BinaryType()
        #"array" : ArrayType(),
        #"map" : MapType()
    }
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        campos = []
        for x, y in columns_info:
            campo = SparkUtils.types.get(y)
            campos.append(StructField(x,campo,True))
        return StructType(campos)