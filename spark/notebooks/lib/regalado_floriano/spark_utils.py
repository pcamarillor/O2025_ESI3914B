
from urllib.parse import urlparse
from pyspark.sql.types import StructField,StructType, StringType, IntegerType, IntegerType, ShortType, DoubleType, FloatType, BooleanType, DateType, FloatType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, MapType

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
        raise NotImplementedError("Not implemented yet")

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        typeMap = {
            "int": IntegerType,
            "string": StringType,
            "double" :DoubleType, 
            "float": FloatType, 
            "bool" : BooleanType, 
            "datetype": DateType, 
            "timestamp": TimestampType,
            "binary": BinaryType, 
            "array": ArrayType, 
            "map" : MapType
        }
        return StructType(
            (list(  map(
                lambda kv: StructField(kv[0], typeMap[kv[1]](), True ),
                columns_info

            )) )
        )

