from pyspark.sql.types import StructType,StructField,StringType,IntegerType

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        typesdict= {
            "string": StringType(),
            "int": IntegerType()
        }

        fields=[]
        for column_name, column_type in columns_info:
            fields.append(StructField(column_name, typesdict[column_type], True))
        return StructType(fields)