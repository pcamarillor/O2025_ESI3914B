from pyspark.sql.types import *

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        dict_type = {
            "string": StringType(),
            "int": IntegerType(),
            "integer": IntegerType(),
            "long": LongType(),
            "short": ShortType(),
            "double": DoubleType(),
            "float": FloatType(),
            "boolean": BooleanType(),
            "date": DateType(),
            "timestamp": TimestampType(),
            "binary": BinaryType(),
        }

        fields = []
        for column_name, str_type in columns_info:
            spark_type = dict_type.get(str_type.lower())
            fields.append(StructField(column_name, spark_type, True))

        return StructType(fields)