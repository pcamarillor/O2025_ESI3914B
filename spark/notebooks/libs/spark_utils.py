from pyspark.sql import SparkSession

def init_spark():
    # Inicializa y retorna una sesión de Spark
    spark = SparkSession.builder.appName('Car_Rental_Analysis').getOrCreate()
    return spark

def load_json_data(spark, file_path):
    # Carga los datos desde un archivo JSON
    return spark.read.json(file_path)
