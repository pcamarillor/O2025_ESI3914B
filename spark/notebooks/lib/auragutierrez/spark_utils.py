from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

class SparkUtils:

    types_dict = {
        "string": StringType(),
        "int": IntegerType(),
        "float": FloatType()
    }

    @staticmethod
    def generate_schema(columns_info) -> StructType:
        """
        Generates a Spark StructType schema from a list of column definitions.

        Args:
            columns_info: A list of tuples, where each tuple contains the column
                          name (string) and the data type (string, e.g., "string", "int").

        Returns:
            A pyspark.sql.types.StructType object.
        """
        schema_fields = []
        for col_name, col_type_str in columns_info:
            if col_type_str in SparkUtils.types_dict:
                spark_type = SparkUtils.types_dict[col_type_str]
                schema_fields.append(StructField(col_name, spark_type, True))
            else:
                raise ValueError(f"Unsupported data type: {col_type_str}")
        
        return StructType(schema_fields)