from pyspark.sql import SparkSession
from etl.extract import Extractor
from etl.utils import setup_logger
import logging

# Configurar logging
logger = setup_logger("main", level=logging.INFO)

# Crear sesión de Spark
spark = SparkSession.builder.appName("ETLExample").getOrCreate()
logger.info("Spark session iniciada")

# Crear extractor
extractor = Extractor(spark)
logger.info("Extractor inicializado")

# Procesar archivo
path = "data/input/sales_uuid.csv"
logger.info(f"Iniciando procesamiento de: {path}")

try:
    df = extractor.run_extract_data_preparation(path)
    logger.info("Procesamiento completado exitosamente")
except Exception as e:
    logger.error(f"Error durante el procesamiento: {str(e)}")
    raise
finally:
    logger.info("Cerrando Spark session")
    spark.stop()







