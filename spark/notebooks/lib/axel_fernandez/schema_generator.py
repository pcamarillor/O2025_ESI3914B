from pyspark.sql.types import StructType
from .spark_utils import SparkUtils

class SchemaGenerator:
    @staticmethod
    def get_logs_schema() -> StructType:
        """Returns a schema for log files with timestamp, log_level, description, and server."""
        return SparkUtils.generate_schema([
            ("timestamp", "timestamp"),
            ("log_level", "string"),
            ("description", "string"),
            ("server", "string")
        ])

    @staticmethod
    def get_drivers_schema() -> StructType:
        """
        Schema para Cityride Drivers Data.csv basado en el CSV en:
        spark/data/final_project/Cityride Drivers Data.csv
        """
        return SparkUtils.generate_schema([
            ("Driver_ID", "int"),
            ("Name", "string"),
            ("Age", "int"),
            ("City", "string"),
            ("Experience_Years", "int"),
            ("Average_Rating", "double"),
            ("Active_Status", "string")
        ])

    @staticmethod
    def get_rides_schema() -> StructType:
        """
        Schema para Cityride Rides Data.csv basado en el CSV en:
        spark/data/final_project/Cityride Rides Data.csv
        Nota: la columna Date se deja como string para parseo posterior en el notebook.
        """
        return SparkUtils.generate_schema([
            ("Ride_ID", "int"),
            ("Driver_ID", "int"),
            ("Ride_City", "string"),
            ("Date", "string"),
            ("Distance_km", "double"),
            ("Duration_min", "int"),
            ("Fare", "double"),
            ("Rating", "double"),
            ("Promo_Code", "string")
        ])