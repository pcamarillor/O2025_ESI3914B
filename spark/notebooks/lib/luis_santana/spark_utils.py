from urllib.parse import urlparse
from pyspark.sql.types import StructType, StringType, IntegerType, IntegerType, ShortType, DoubleType, FloatType, BooleanType, DateType, FloatType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, MapType




# Method to generate StructType from a list of (column_name, type_string) tuples
class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        