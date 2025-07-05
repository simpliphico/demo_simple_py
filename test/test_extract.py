import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from src.etl.extract import Extractor
from chispa import assert_df_equality



@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("extract-tests") \
        .master("local[1]") \
        .getOrCreate()

def test_spark_basic(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    assert df.count() == 2
