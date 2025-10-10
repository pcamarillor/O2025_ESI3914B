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