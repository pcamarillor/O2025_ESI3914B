from urllib.parse import urlparse
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, IntegerType, ShortType, DoubleType, FloatType, BooleanType, DateType, FloatType, BooleanType, DateType, TimestampType, BinaryType

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
        # canonical instances
        _types = {
            "StringType": StringType(),
            "IntegerType": IntegerType(),
            "ShortType": ShortType(),
            "DoubleType": DoubleType(),
            "FloatType": FloatType(),
            "BooleanType": BooleanType(),
            "DateType": DateType(),
            "TimestampType": TimestampType(),
            "BinaryType": BinaryType(),
        }

        # common aliases → canonical keys (lowercase on the left)
        _aliases = {
            "string": "StringType",
            "str": "StringType",
            "int": "IntegerType",
            "integer": "IntegerType",
            "short": "ShortType",
            "double": "DoubleType",
            "float": "FloatType",
            "bool": "BooleanType",
            "boolean": "BooleanType",
            "date": "DateType",
            "timestamp": "TimestampType",
            "binary": "BinaryType",
        }

        fields = []
        for name, type_str in columns_info:
            key = str(type_str).strip()
            canonical = key if key in _types else _aliases.get(key.lower())
            if canonical not in _types:
                supported = ", ".join(sorted(_types.keys() | set(_aliases.keys())))
                raise ValueError(
                    f"Unsupported type '{type_str}' for column '{name}'. Supported: {supported}"
                )

            fields.append(StructField(name, _types[canonical], nullable=True))

        return StructType(fields)

