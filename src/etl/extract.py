from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, coalesce
from pyspark.sql.column import Column
from etl.utils import get_logger
import os

files_metadata = [
    {
        "filename": "products_uuid.csv",
        "folder": "data/input",
        "columns": [
            {"name": "product_id", "type": "string"},
            {"name": "product_name", "type": "string"},
            {"name": "category", "type": "string"},
        ],
    },
    {
        "filename": "stores_uuid.csv",
        "folder": "data/input",
        "columns": [
            {"name": "store_id", "type": "string"},
            {"name": "store_name", "type": "string"},
            {"name": "location", "type": "string"},
        ],
    },
    {
        "filename": "sales_uuid.csv",
        "folder": "data/input",
        "columns": [
            {"name": "transaction_id", "type": "string"},
            {"name": "store_id", "type": "string"},
            {"name": "product_id", "type": "string"},
            {"name": "quantity", "type": "integer"},
            {"name": "transaction_date", "type": "date"},
            {"name": "price", "type": "double"},
        ],
    },
]


class Extractor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = get_logger("extractor")

    def load_csv(
        self, file_path: str,
        header: bool = True
    ) -> DataFrame:
        """Loads a CSV file using the specified schema."""
        return self.spark.read.csv(file_path, header=header)

    def get_metadata_file(self, filename: str) -> dict:
        for file in files_metadata:
            if file["filename"] == filename:
                return file
        raise ValueError(f"File not found: {filename}")

    def get_file_list(self) -> list[str]:
        filenames = [file["filename"] for file in files_metadata]
        return filenames

    def get_metadata_file_folder(self, filename: str) -> str:
        for file in files_metadata:
            if file["filename"] == filename:
                folder = file["folder"]
                return folder
        raise ValueError(f"File not found: {filename}")

    def format_date(self, column_name: Column) -> Column:
        formats_list = [
            "yyyy/MM/dd", "yy/MM/dd", "yyyy-MM-dd",
            "MM/dd/yyyy", "dd-MM-yyyy", "MM-dd-yyyy", "MMMM dd, yyyy"
        ]
        try:
            date_formats = [to_date(column_name, format) for format in formats_list]
            return coalesce(*date_formats)
        except Exception as e:
            raise ValueError(
                f"Invalid date format for column {column_name}: {str(e)}"
            )

    def validate_and_clean_columns(self, df: DataFrame, filename: str) -> DataFrame:
        file_info = self.get_metadata_file(filename)
        for column in file_info["columns"]:
            if column["name"] not in df.columns:
                raise ValueError(
                    f"Column {column['name']} not found in file"
                    )
            if column["type"] == "date":
                df = df.withColumn(column["name"], self.format_date(column["name"]))
            elif column["type"] == "integer":
                df = df.withColumn(column["name"], col(column["name"]).cast("integer"))
            elif column["type"] == "double":
                df = df.withColumn(column["name"], col(column["name"]).cast("double"))
            else:
                self.logger.info(
                    f"No transformation needed for column {column['name']}"
                    )
        return df

    def clean_column_names(self, df: DataFrame) -> DataFrame:
        new_names = [c.strip().lower().replace(" ", "_") for c in df.columns]
        for old, new in zip(df.columns, new_names):
            df = df.withColumnRenamed(old, new)
        return df

    def check_missing_values(self, df: DataFrame) -> DataFrame:
        null_columns = [col(c).isNull().cast("int").alias(c) for c in df.columns]
        return df.select(null_columns).groupBy().sum()

    def drop_duplicates(self, df: DataFrame) -> DataFrame:
        return df.dropDuplicates()

    def generate_path(self, filename: str, folder: str) -> str:
        """Returns the complete file path by combining folder and filename."""
        path = os.path.normpath(os.path.join(folder, filename))
        return path

    def run_extract_data_preparation_file(
        self, filename: str, folder: str
        ) -> DataFrame:

        file_path = self.generate_path(filename, folder)

        self.logger.info(f"Loading: {file_path}")
        df = self.load_csv(file_path)

        self.logger.info("Cleaning column names...")
        df = self.clean_column_names(df)

        self.logger.info("Validating and cleaning columns...")
        df = self.validate_and_clean_columns(df, filename)

        self.logger.info("Checking missing values...")
        nulls = self.check_missing_values(df)
        nulls.show()

        self.logger.info("Removing duplicates...")
        df = self.drop_duplicates(df)

        self.logger.info("Pipeline completed.")
        return df

def data_preparation(spark):
    logger_prep = get_logger("data_preparation")
    extractor = Extractor(spark)
    logger_prep.info("Extractor initialized")

    filenames = extractor.get_file_list()

    try:
        # Dictionary to store DataFrames with dynamic names
        dataframes = {}

        for filename in filenames:
            folder = extractor.get_metadata_file_folder(filename)
            logger_prep.info(
                f"Starting processing of: {filename} located in {folder}"
            )
            df = extractor.run_extract_data_preparation_file(filename, folder)

            # Create variable name based on filename without .csv extension
            filename_without_extension = filename.replace('.csv', '')
            df_variable_name = f"df_{filename_without_extension}"

            # Store DataFrame in dictionary with the dynamic name
            dataframes[df_variable_name] = df

            logger_prep.info(f"Created DataFrame: {df_variable_name}")

        # Extract DataFrames to individual variables
        df_sales_uuid = dataframes.get('df_sales_uuid')
        df_sales_uuid.show()
        df_products_uuid = dataframes.get('df_products_uuid')
        df_products_uuid.show()
        df_store_uuid = dataframes.get('df_stores_uuid')
        df_store_uuid.show()

        logger_prep.info("Processing completed successfully")
        logger_prep.info(
            f"Available DataFrames: {list(dataframes.keys())}"
        )

    except Exception as e:
        logger_prep.error(f"Error during processing: {str(e)}")
        raise


