
from urllib.parse import urlparse
from pyspark.sql.types import StructField,StructType, StringType, IntegerType, IntegerType, ShortType, DoubleType, FloatType, BooleanType, DateType, FloatType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, MapType
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import col

def parse_line(line):
    parts = line.strip().split(",")
    return (parts[0], parts[1], parts[2])

def is_yesterday(date_str, yesterday):
    return date_str.split('T', 1)[0] == yesterday

def to_domain(url):
    host = urlparse(url).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host



class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        raise NotImplementedError("Not implemented yet")

class SparkUtils:

    @staticmethod
    def generate_keyed_distinct_column(df):
        """curried for ease of use"""
        def get(col_name):
            dist = df.select(col(col_name)).distinct()
            return dist.withColumn('id', monotonically_increasing_id())
        return get

    @staticmethod
    def replace_column_for_key(main_df):
        def get_map_df(key_val_df):
            def get_cokey(name):
                res = main_df.alias("m").join(
                    key_val_df.alias("n"), 
                    col(f"n.{name}") == col(f"m.{name}"), 
                    "left"
                    ).drop(f"{name}").withColumnRenamed("id",f"{name}_id")

                return res

            return get_cokey
        return get_map_df

    @staticmethod
    def writeToPostGres(df):
        def get_endpoint(jdbc_url):
            def get_name(name):
                df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", name) \
                .option("user", "postgres") \
                .option("password", "Admin@1234") \
                .option("driver", "org.postgresql.Driver") \
                .save()
            return get_name
        return get_endpoint



    @staticmethod
    def generate_schema(columns_info) -> StructType:
        typeMap = {
            "int": IntegerType,
            "string": StringType,
            "double" :DoubleType, 
            "float": FloatType, 
            "bool" : BooleanType, 
            "datetype": DateType, 
            "timestamp": TimestampType,
            "binary": BinaryType, 
            "array": ArrayType, 
            "map" : MapType
        }
        return StructType(
            (list(  map(
                lambda kv: StructField(kv[0], typeMap[kv[1]](), True ),
                columns_info

            )) )
        )

