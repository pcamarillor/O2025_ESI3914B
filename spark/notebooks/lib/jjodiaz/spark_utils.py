from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType,FloatType, TimestampType
)

class SparkUtils:
    types_dict = {
        "string": StringType(),
        "int": IntegerType(), 
        "double": DoubleType(),
        "float": FloatType(),
        "timestamp": TimestampType()
    }
    
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        fields = []
        
        for column_name, data_type in columns_info:
            spark_type = SparkUtils.types_dict[data_type]
            
            field = StructField(column_name, spark_type, True)
            fields.append(field)
        
        return StructType(fields)