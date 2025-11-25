from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        # Diccionario que contiene los tipos soportados
        types_dict = {
            "string": StringType(),
            "int": IntegerType(),
            "float": FloatType(),
            "boolean": BooleanType()
        }
        
        # Lista para almacenar los StructField
        fields = []
        
        # Iteramos sobre la lista de tuplas para crear el esquema
        for column_name, column_type in columns_info:
            if column_type.lower() in types_dict:
                fields.append(StructField(column_name, types_dict[column_type.lower()]))
            else:
                raise ValueError(f"Tipo de dato {column_type} no soportado")
        
        # Retornamos el StructType creado
        return StructType(fields)
