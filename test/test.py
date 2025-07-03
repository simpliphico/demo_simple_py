from pyspark.sql import SparkSession
from chispa import assert_df_equality
import pytest

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("DummyTest") \
        .master("local[*]") \
        .getOrCreate()

def test_dummy_dataframe_equality(spark):
    data = [("Ana", 25), ("Luis", 30)]
    columns = ["name", "age"]

    df1 = spark.createDataFrame(data, columns)
    df2 = spark.createDataFrame(data, columns)

    # Validación con chispa
    assert_df_equality(df1, df2)
