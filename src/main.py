from pyspark.sql import SparkSession
from etl.extract import Extractor
from etl.utils import setup_logger
import logging

def main():
    logger = setup_logger("main", level=logging.INFO)
    spark = SparkSession.builder.appName("ETLExample").getOrCreate()
    logger.info("Spark session started")

    extractor = Extractor(spark)
    logger.info("Extractor initialized")

    filename = "sales_uuid.csv"
    folder = "data/input"
    logger.info(f"Starting processing of: {filename} located in {folder}")

    try:
        df = extractor.run_extract_data_preparation(filename, folder)
        df.show()
        logger.info("Processing completed successfully")
    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        raise
    finally:
        logger.info("Closing Spark session")
        spark.stop()

if __name__ == "__main__":
    main()
