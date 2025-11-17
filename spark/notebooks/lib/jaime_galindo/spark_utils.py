from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ShortType, DoubleType, FloatType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, DataType # type: ignore


class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        """
        Genera un StructType a partir de una lista de tuplas.
        Acepta strings de tipo (ej. "string") o tipos complejos
        de Spark (ej. ArrayType(...), StructType(...)).

        Args:
            columns_info (list of tuples): Cada tupla contiene (column_name, data_type).
                                 data_type puede ser un string o un objeto DataType.
        
        Returns:
            StructType: El esquema de Spark.
        """
        # Diccionario de tipos soportados
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
        }

        fields = []
        for name, type_str in columns_info:
            # Verificar si es tipo de dato simple
            if isinstance(type_str, str):
                if type_str not in types_dict:
                    raise ValueError(f"Tipo de dato no soportado: {type_str}")
                spark_type = types_dict[type_str]
            
            # Si es un tipo de Spark (como ArrayType o StructType)
            elif isinstance(type_str, DataType):
                spark_type = type_str
            
            # Si no es ni string ni DataType, es un error
            else:
                raise TypeError(f"Tipo de columna no válido para '{name}': {type(type_str)}")

            fields.append(StructField(name, spark_type, True)) 
        return StructType(fields)

