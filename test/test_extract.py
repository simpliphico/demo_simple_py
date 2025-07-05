import pytest
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col
from etl.extract import Extractor
from chispa import assert_df_equality


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("extract-tests") \
        .master("local[1]") \
        .getOrCreate()


@pytest.fixture
def extractor(spark):
    return Extractor(spark)


def test_spark_basic(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    assert df.count() == 2


def test_get_metadata_file(extractor):
    # Test for existing file
    metadata = extractor.get_metadata_file("products_uuid.csv")
    assert metadata["filename"] == "products_uuid.csv"
    assert len(metadata["columns"]) == 3
    assert metadata["columns"][0]["name"] == "product_id"

    # Test for non-existent file
    with pytest.raises(ValueError, match="File not found: nonexistent.csv"):
        extractor.get_metadata_file("nonexistent.csv")


def test_clean_column_names(extractor, spark):
    # Create DataFrame with dirty column names
    input_df = spark.createDataFrame(
        data=[("1", "Producto 1", "Cat A")],
        schema=StructType([
            StructField(" Product ID ", StringType(), True),
            StructField("Product Name", StringType(), True),
            StructField("  Category  ", StringType(), True)
        ])
    )

    # Expected DataFrame with clean names
    expected_df = spark.createDataFrame(
        data=[("1", "Producto 1", "Cat A")],
        schema=["product_id", "product_name", "category"]
    )

    cleaned_df = extractor.clean_column_names(input_df)

    # Verify that columns were cleaned correctly
    assert cleaned_df.columns == ["product_id", "product_name", "category"]
    assert_df_equality(cleaned_df, expected_df, ignore_nullable=True)


def test_check_missing_values(extractor, spark):
    # Create DataFrame with null values
    input_df = spark.createDataFrame(
        data=[
            (None, "prod1", None, 10),
            ("1", "prod2", "store1", 20),
            ("2", None, "store2", None)
        ],
        schema=["transaction_id", "product_id", "store_id", "price"]
    )

    result_df = extractor.check_missing_values(input_df)
    row = result_df.first().asDict()

    # Verify null value counts
    assert row["sum(transaction_id)"] == 1  # 1 null value
    assert row["sum(product_id)"] == 1      # 1 null value
    assert row["sum(store_id)"] == 1        # 1 null value
    assert row["sum(price)"] == 1           # 1 null value


def test_drop_duplicates(extractor, spark):
    # Create DataFrame with duplicates
    input_df = spark.createDataFrame(
        [("1", "prod1"), ("1", "prod1"), ("2", "prod2"), ("2", "prod2")],
        ["transaction_id", "product_id"]
    )

    result_df = extractor.drop_duplicates(input_df)

    # Verify that duplicates were removed
    assert result_df.count() == 2
    rows = result_df.collect()
    assert len(rows) == 2


def test_generate_path(extractor):
    # Test for generating file paths
    path = extractor.generate_path("test.csv", "data/input")
    # Use os.path.normpath to handle different directory separators
    expected_path = os.path.normpath("data/input/test.csv")
    assert path == expected_path

    # Test with different directory separators
    path2 = extractor.generate_path("file.txt", "folder/subfolder")
    expected_path2 = os.path.normpath("folder/subfolder/file.txt")
    assert path2 == expected_path2


def test_validate_and_clean_columns_products(extractor, spark):
    # Test for products file
    input_df = spark.createDataFrame(
        data=[("prod1", "Producto 1", "Electronics")],
        schema=["product_id", "product_name", "category"]
    )

    result_df = extractor.validate_and_clean_columns(input_df, "products_uuid.csv")

    # Verify that columns are present and with correct types
    assert "product_id" in result_df.columns
    assert "product_name" in result_df.columns
    assert "category" in result_df.columns


def test_validate_and_clean_columns_sales(extractor, spark):
    # Test for sales file with specific data types
    input_df = spark.createDataFrame(
        data=[("trans1", "store1", "prod1", "5", "2023-01-01", "10.50")],
        schema=[
            "transaction_id", "store_id",
            "product_id", "quantity", "transaction_date", "price"
            ]
    )

    result_df = extractor.validate_and_clean_columns(input_df, "sales_uuid.csv")

    # Verify that columns are present
    assert "transaction_id" in result_df.columns
    assert "store_id" in result_df.columns
    assert "product_id" in result_df.columns
    assert "quantity" in result_df.columns
    assert "transaction_date" in result_df.columns
    assert "price" in result_df.columns


def test_validate_and_clean_columns_missing_column(extractor, spark):
    # Test to verify that error is raised when a column is missing
    input_df = spark.createDataFrame(
        data=[("prod1", "Producto 1")],  # Missing "category" column
        schema=["product_id", "product_name"]
    )

    with pytest.raises(ValueError, match="Column category not found in file"):
        extractor.validate_and_clean_columns(input_df, "products_uuid.csv")


def test_format_date_success(extractor, spark):
    # Test for successful date formatting
    input_df = spark.createDataFrame(
        data=[("2023-01-01",), ("2023/01/02",), ("01/03/2023",)],
        schema=["date_col"]
    )

    # Create a date column
    date_col = col("date_col")
    formatted_col = extractor.format_date(date_col)

    # Apply the transformation
    result_df = input_df.withColumn("formatted_date", formatted_col)

    # Verify that the column was created
    assert "formatted_date" in result_df.columns


def test_load_csv(extractor, spark, tmp_path):
    # Test for CSV loading
    # Create a temporary CSV file
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("id,name\n1,test1\n2,test2")

    df = extractor.load_csv(str(csv_file), header=True)

    # Verify that it loaded correctly
    assert df.count() == 2
    assert "id" in df.columns
    assert "name" in df.columns


def test_run_extract_data_preparation_integration(extractor, spark, tmp_path):
    # Integration test for the complete pipeline
    # Create temporary CSV file
    csv_file = tmp_path / "products_uuid.csv"
    csv_file.write_text(
        "product_id,product_name,category\nprod1,\
        Producto 1,Electronics\nprod2,Producto 2,Books"
        )

    # Run the complete pipeline
    result_df = extractor.run_extract_data_preparation(
        "products_uuid.csv", str(tmp_path)
        )

    # Verify results
    assert result_df.count() == 2
    assert "product_id" in result_df.columns
    assert "product_name" in result_df.columns
    assert "category" in result_df.columns
