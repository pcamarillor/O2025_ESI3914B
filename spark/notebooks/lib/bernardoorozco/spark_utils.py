from pyspark.sql.types import StructType,StructField,StringType,IntegerType, FloatType, DoubleType, BooleanType, DateType, TimestampType

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        typesdict = {
            "string": StringType(),
            "int": IntegerType(),
            "integer": IntegerType(),
            "float": FloatType(),
            "double": DoubleType(),
            "boolean": BooleanType(),
            "date": DateType(),
            "timestamp": TimestampType()
        }

        fields=[]
        for column_name, column_type in columns_info:
            fields.append(StructField(column_name, typesdict[column_type], True))
        return StructType(fields)