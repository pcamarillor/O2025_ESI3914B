from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ShortType, DoubleType, FloatType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, MapType



class SparkUtils:
    types = {
        "struct": StructType,
        "string": StringType(),
        "int": IntegerType(),
        "short": ShortType(),
        "double": DoubleType(),
        "float": FloatType(), 
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "binary": BinaryType(),
        "array": ArrayType,
        "map": MapType
    }

    @staticmethod
    def generate_schema(columns_info) -> StructType:

        rows = []

        for row in columns_info:
            if len(row) == 2:
                value, field_type = row
                nullable = True
            else:
                value, field_type, nullable = row

            rows.append(StructField(value, SparkUtils.types[field_type.lower()], nullable))

        return StructType(rows)