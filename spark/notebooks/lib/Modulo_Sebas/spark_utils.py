from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        types_dict = {
            "string": StringType(),
            "int": IntegerType(),
            "StringType": StringType(),
            "IntegerType": IntegerType()
        }
        fields = []
        for name, type_str in columns_info:
            key = type_str.strip()
            if key not in types_dict:
                raise ValueError(f"Unsupported type: {type_str}")
            fields.append(StructField(name, types_dict[key], True))
        return StructType(fields)
