from urllib.parse import urlparse
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, IntegerType, ShortType, DoubleType, FloatType, BooleanType, DateType, FloatType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, MapType

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

def generate_schema(cls, fields):

        struct_fields = []
        for col_name, type_str in fields:
            if type_str not in cls.types_dict:
                raise ValueError(f"Tipo no soportado: {type_str}")
            struct_fields.append(
                StructField(col_name, cls.types_dict[type_str], True)
            )
        return StructType(struct_fields)

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        raise NotImplementedError("Not implemented yet")


