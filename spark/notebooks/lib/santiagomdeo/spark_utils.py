from urllib.parse import urlparse
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ShortType, DoubleType, FloatType, BooleanType, DateType, FloatType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, MapType

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

def proof():
    print("import works")




type_map = {
    "string": StringType,
    "int": IntegerType,
    "short": ShortType,
    "double": DoubleType,
    "float": FloatType,
    "boolean": BooleanType,
    "date": DateType,
    "timestamp": TimestampType,
    "binary": BinaryType,
    "array": ArrayType,
    "map": MapType
    }   

def get_type(type_str: str):
    print(type_str)
    if type_str in type_map:
        return type_map[type_str]()  # call the constructor
    else:
        raise ValueError(f"Unknown type: {type_str}")

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        list_of_columns = []
        for x in columns_info:
            list_of_columns.append(StructField(x[0], get_type(x[1]), True))

