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
        types_dict = {
            "StringType": StringType(),
            "IntegerType": IntegerType(),
            "ShortType": ShortType(),
            "DoubleType": DoubleType(),
            "FloatType": FloatType(),
            "BooleanType": BooleanType(),
            "DateType": DateType(),
            "TimestampType": TimestampType(),
            "BinaryType": BinaryType()
        }

        fields = []
        for item in columns_info:
            col_name, type_str = item[0], item[1]

            if type_str not in types_dict:
                supported = ", ".join(sorted(types_dict.keys()))
                raise ValueError(f"Unsupported type '{type_str}' for column '{col_name}'. "
                                 f"Supported: {supported}")

            fields.append(StructField(col_name, types_dict[type_str]))

        return StructType(fields)

