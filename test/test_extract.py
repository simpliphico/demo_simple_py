import pytest
from pyspark.sql import SparkSession



@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("extract-tests") \
        .master("local[1]") \
        .getOrCreate()

def test_spark_basic(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    assert df.count() == 2
