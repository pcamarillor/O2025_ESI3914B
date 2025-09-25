from urllib.parse import urlparse
from pyspark.sql.types import StructType, StringType, IntegerType, IntegerType, ShortType, DoubleType, FloatType, BooleanType, DateType, FloatType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, MapType
from pyspark.sql.types import StructField



# Method to generate StructType from a list of (column_name, type_string) tuples
class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        types_dict = {
            "string": StringType(),
            "int": IntegerType(),
            "short": ShortType(),
            "double": DoubleType(),
            "float": FloatType(),
            "bollean": BooleanType(),
            "date": DateType(),
            "timestamp": TimestampType(),
            "bool": BinaryType(),
            "array_int": ArrayType(IntegerType()),
            "array_string": ArrayType(StringType()),
            "struct": StructType()
        }
        fields = []
        for col,type in columns_info:
            spark_type = types_dict.get(type)
            if not spark_type:
                raise ValueError(f"Unsupported column type: {type}")
            fields.append(StructField(col, spark_type, True)) 
        # print(fields)
        schema = StructType(fields)
        return schema
        
