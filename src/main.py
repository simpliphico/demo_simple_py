from pyspark.sql import SparkSession
from etl.extract import data_preparation
from etl.utils import setup_logger
import logging

def main():
    logger = setup_logger("main", level=logging.INFO)
    spark = SparkSession.builder.appName("ETLExample").getOrCreate()
    logger.info("Spark session started")

    data_preparation(spark)
    logger.info("Closing Spark session")
    spark.stop()

if __name__ == "__main__":
    main()
