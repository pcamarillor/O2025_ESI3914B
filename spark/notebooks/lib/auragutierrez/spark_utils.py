from urllib.parse import urlparse
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ShortType, DoubleType,
    FloatType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, MapType
)

def parse_line(line):
    parts = line.strip().split(",")
    return (parts[0], parts[1], parts[2])

def is_yesterday(date_str, yesterday):
    return date_str.split('T', 1)[0] == yesterday

def to_domain(url):
    host = urlparse(url).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host

class SparkUtils:
    
    types_dict = {
        "string": StringType(),
        "int": IntegerType(),
        "short": ShortType(),
        "double": DoubleType(),
        "float": FloatType(),
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "binary": BinaryType(),
        "array": ArrayType(StringType()),
        "map": MapType(StringType(), StringType())
    }

    @staticmethod
    def generate_schema(columns_info) -> StructType:
        schema_fields = []
        for col_name, col_type_str in columns_info:
            col_type_str_lower = col_type_str.lower()
            if col_type_str_lower in SparkUtils.types_dict:
                spark_type = SparkUtils.types_dict[col_type_str_lower]
                schema_fields.append(StructField(col_name, spark_type, True))
            else:
                raise ValueError(f"Unsupported data type: {col_type_str}")
        
        return StructType(schema_fields)