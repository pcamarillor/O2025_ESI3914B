from pyspark.sql.types import StructType, StringType, IntegerType, IntegerType, ShortType, DoubleType, FloatType, BooleanType, DateType, FloatType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, MapType, LongType, StructField

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        types_dict = {
            "string": StringType(),
            "int": IntegerType(),
            "long": LongType(),
            "short": ShortType(),
            "double": DoubleType(),
            "float": FloatType(),
            "boolean": BooleanType(),
            "date": DateType(),
            "timestamp": TimestampType(),
            "binary": BinaryType(),
            "array": ArrayType,
            "map": MapType,
            "struct": StructType
        }
        structs = []
        for column_name, column_type in columns_info:
            spark_type = types_dict.get(column_type)
            structs.append(StructField(column_name, spark_type, True))
        return StructType(structs)
