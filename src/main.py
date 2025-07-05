from pyspark.sql import SparkSession
from etl.extract import Extractor
from etl.utils import setup_logger
import logging

def main():
    logger = setup_logger("main", level=logging.INFO)
    spark = SparkSession.builder.appName("ETLExample").getOrCreate()
    logger.info("Spark session iniciada")

    extractor = Extractor(spark)
    logger.info("Extractor inicializado")

    filename = "sales_uuid.csv"
    folder = "data/input"
    logger.info(f"Iniciando procesamiento de: {filename} situado en {folder}")

    try:
        df = extractor.run_extract_data_preparation(filename, folder)
        df.show()
        logger.info("Procesamiento completado exitosamente")
    except Exception as e:
        logger.error(f"Error durante el procesamiento: {str(e)}")
        raise
    finally:
        logger.info("Cerrando Spark session")
        spark.stop()

if __name__ == "__main__":
    main()
