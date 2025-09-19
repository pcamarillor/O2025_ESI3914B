from pyspark.sql.types import (
    StringType,
    IntegerType,
    FloatType,
    DoubleType,
    BooleanType,
    LongType,
    ShortType,
    ByteType,
    DateType,
    TimestampType,
    StructType,
    StructField
)


class SparkUtils:
    types_dict = {
        "StringType": StringType(),
        "IntegerType": IntegerType(),
        "FloatType": FloatType(),
        "DoubleType": DoubleType(),
        "BooleanType": BooleanType(),
        "LongType": LongType(),
        "ShortType": ShortType(),
        "ByteType": ByteType(),
        "DateType": DateType(),
        "TimestampType": TimestampType()
    }

    @classmethod
    def generate_schema(cls, columns):
        fields = []
        for name, type_str in columns:
            data_type = cls.types_dict.get(type_str)
            if not data_type:
                raise ValueError(f"Unsupported data type: {type_str}")
            fields.append(StructField(name, data_type, True))
        return StructType(fields)
