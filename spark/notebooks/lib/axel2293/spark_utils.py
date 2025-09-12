from pyspark.sql.types import StructType, StructField, StringType, IntegerType, IntegerType, LongType, ShortType, DoubleType, FloatType, BooleanType, DateType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, MapType


class SparkUtils:
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
        "binary": BinaryType()
    }
    
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        # My logic here
        fd = []
        for col, d_type in columns_info:
            # Validate if its a valid type
            if d_type not in SparkUtils.types_dict:
                raise ValueError("No soportado amigo :(")
            # Create the Struct Field
            field = StructField(col, SparkUtils.types_dict[d_type], True)
            fd.append(field)


        # Return the Struct Type
        return StructType(fd)
        
        