from urllib.parse import urlparse
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, IntegerType, ShortType, DoubleType, FloatType, BooleanType, DateType, FloatType, BooleanType, DateType, TimestampType, BinaryType
from pyspark.sql import SparkSession, functions as F

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

def drop_unnecessary(df):
    return df.drop("flight", "class") if all(c in df.columns for c in ["flight", "class"]) else df

def count_nulls(df):
    return df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])

def normalize_stops(df):
    mapping = {
        "zero": 0, "non-stop": 0,
        "one": 1,
        "two_or_more": 2, "2_or_more": 2, "two or more": 2,
    }
    mapping_expr = F.create_map([F.lit(x) for kv in mapping.items() for x in kv])
    return df.withColumn("stops", mapping_expr.getItem(F.lower(F.col("stops"))).cast("int"))

def add_route(df):
    return df.withColumn("route", F.concat_ws(" → ", F.col("source_city"), F.col("destination_city")))

def encode_times(df):
    order = ["Early_Morning", "Morning", "Afternoon", "Evening", "Night", "Late_Night"]
    mapping_expr = F.create_map([F.lit(x) for kv in enumerate(order) for x in (kv[1].lower(), kv[0])])

    df = df.withColumn("departure_time_id", mapping_expr.getItem(F.lower(F.col("departure_time"))))
    df = df.withColumn("arrival_time_id", mapping_expr.getItem(F.lower(F.col("arrival_time"))))
    return df

def add_is_expensive(df):
    return df.withColumn("is_expensive", (F.col("price") > 6000))

def avg_price_per_airline(df):
    return df.groupBy("airline").agg(F.avg("price").alias("avg_price"))

def avg_duration_per_route(df):
    return df.groupBy("route").agg(F.avg("duration").alias("avg_duration"))

def min_max_price_per_airline(df):
    return df.groupBy("airline").agg(
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price")
    )

def count_by_departure(df):
    return df.groupBy("departure_time").count()

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        _types = {
            "StringType": StringType(),
            "IntegerType": IntegerType(),
            "ShortType": ShortType(),
            "DoubleType": DoubleType(),
            "FloatType": FloatType(),
            "BooleanType": BooleanType(),
            "DateType": DateType(),
            "TimestampType": TimestampType(),
            "BinaryType": BinaryType(),
        }

        _aliases = {
            "string": "StringType",
            "str": "StringType",
            "int": "IntegerType",
            "integer": "IntegerType",
            "short": "ShortType",
            "double": "DoubleType",
            "float": "FloatType",
            "bool": "BooleanType",
            "boolean": "BooleanType",
            "date": "DateType",
            "timestamp": "TimestampType",
            "binary": "BinaryType",
        }

        fields = []
        for name, type_str in columns_info:
            key = str(type_str).strip()
            canonical = key if key in _types else _aliases.get(key.lower())
            if canonical not in _types:
                supported = ", ".join(sorted(_types.keys() | set(_aliases.keys())))
                raise ValueError(
                    f"Unsupported type '{type_str}' for column '{name}'. Supported: {supported}"
                )

            fields.append(StructField(name, _types[canonical], nullable=True))

        return StructType(fields)

