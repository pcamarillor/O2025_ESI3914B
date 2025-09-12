
from pyspark.sql.types import StringType, IntegerType, LongType, ShortType, DoubleType, FloatType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, MapType, StructType, StructField

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        type_mapping = {
            "string": StringType(),
            "int": IntegerType(),
            "long": LongType(),
            "short": ShortType(),
            "double": DoubleType(),
            "float": FloatType(),
            "bool": BooleanType(),
            "date": DateType(),
            "timestamp": TimestampType(),
            "binary": BinaryType(),
        }

        fields = []
        for col_name, col_type in columns_info:
            if col_type not in type_mapping:
                raise ValueError(f"Unsuported data type: {col_name}")
            fields.append(StructField(col_name, type_mapping[col_type], True))
            
        return StructType(fields)