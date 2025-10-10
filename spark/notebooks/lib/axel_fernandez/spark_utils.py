from urllib.parse import urlparse
from pyspark.sql.types import StructType, StringType, IntegerType, LongType, ShortType, DoubleType, FloatType, BooleanType, DateType, TimestampType, BinaryType, StructField, ArrayType

class SparkUtils:
    type_mapping = {
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
        "array_int": ArrayType(IntegerType()),
        "array_string": ArrayType(StringType())
    }
    
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        struct_fields = []
        
        for column_info in columns_info:
            if len(column_info) == 2:
                column_name, data_type = column_info
                nullable = True
            else:
                column_name, data_type, nullable = column_info
            
            data_type = data_type.lower()
            if data_type.endswith("type"):
                data_type = data_type[:-4]
            
            if data_type not in SparkUtils.type_mapping:
                raise ValueError(f"Unsupported data type: {data_type}")
            
            field = StructField(column_name, SparkUtils.type_mapping[data_type], nullable)
            struct_fields.append(field)
        
        return StructType(struct_fields)

    @staticmethod # Method to get the schema for the logs lab07
    def get_logs_schema():
        return SparkUtils.generate_schema([
            ("timestamp", "timestamp"),
            ("log_level", "string"),
            ("description", "string"),
            ("server", "string")
        ])
