from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ShortType, DoubleType, FloatType, BooleanType, DateType, TimestampType, BinaryType # type: ignore


class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        """
        Generates a list of StructField objects from a list of tuples.

        Args:
            column_info (list of tuples): Each tuple contains (column_name, data_type_string).

        Returns:
            list: A list of StructField objects.
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
            if type_str not in types_dict:
                raise ValueError(f"Tipo de dato no soportado: {type_str}")
            fields.append(StructField(name, types_dict[type_str], True)) 

        return StructType(fields)

