from urllib.parse import urlparse
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, ShortType, DoubleType, FloatType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, MapType

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
        dict_types = {
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

        fields = []
        for column_name, type_string in columns_info:
            if type_string not in dict_types:
                raise ValueError(f"Unsupported data type: {type_string}")
            
            field = StructField(column_name, dict_types[type_string], True)
            fields.append(field)
        
        return StructType(fields)