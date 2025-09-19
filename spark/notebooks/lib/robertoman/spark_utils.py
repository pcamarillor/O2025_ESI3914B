# importamos todos los tipos de datos que Pyspark peude usar en un df

from pyspark.sql.types import (
    StructType, # esquema completo
    StructField, # representa una columna en el scheme
    StringType,
    IntegerType,
    IntegerType,
    ShortType,
    DoubleType,
    FloatType,
    BooleanType,
    DateType,
    FloatType,
    BooleanType,
    DateType,
    TimestampType,
    BinaryType,
    ArrayType,
    MapType,
)


class SparkUtils:
    # Diccionario con tipos soportados
    types_dict = {
        "string": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "short": ShortType(),
        "double": DoubleType(),
        "float": FloatType(),
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "binary": BinaryType(),
        # "array": ArrayType(),
        # "map": MapType(),
    }

    @staticmethod
    def generate_schema(columns_info) -> StructType:
        fields = []
        for name, type_str in columns_info:
            data_type = SparkUtils.types_dict.get(type_str.lower())
            if data_type is None:
                raise ValueError(f"Data type not supported: {type_str}")
            fields.append(StructField(name, data_type, True))
        return StructType(fields)
