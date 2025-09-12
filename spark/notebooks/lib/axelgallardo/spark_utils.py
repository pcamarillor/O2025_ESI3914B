from urllib.parse import urlparse
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ShortType, 
    DoubleType, FloatType, BooleanType, DateType, TimestampType, 
    BinaryType, ArrayType, MapType
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
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        types_dict = {
            "string": StringType(),
            "int": IntegerType(),
            "integer": IntegerType(),
            "short": ShortType(),
            "double": DoubleType(),
            "float": FloatType(),
            "boolean": BooleanType(),
            "date": DateType(),
            "timestamp": TimestampType(),
            "binary": BinaryType()
        }

        fields = []
        for col_name, col_type in columns_info:
            col_type_lower = col_type.lower()
            if col_type_lower not in types_dict:
                raise ValueError(f"Unsupported type: {col_type}")
            fields.append(
                StructField(col_name, types_dict[col_type_lower], True)
            )

        return StructType(fields)