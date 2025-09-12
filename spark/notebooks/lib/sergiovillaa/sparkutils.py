
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType, TimestampType, StructType, StructField

class SparkUtils:
    def generate_schema(self, columns_with_types: list[tuple]) -> StructType:
        type_mapping = {
            "StringType": StringType,
            "IntegerType": IntegerType,
            "DateType": DateType,
            "TimestampType": TimestampType,
            "FloatType": FloatType
            # Add more types as needed
        }

        schema = StructType()
        for col_name, type_str in columns_with_types:
            if type_str not in type_mapping:
                raise ValueError(f"Unsupported data type: {type_str}")
            
            data_type = type_mapping[type_str]()
            
            schema.add(StructField(col_name, data_type, True))
            
        return schema