from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, StructField, DateType
from pyspark.sql.functions import col
from src.etl.utils import get_logger


product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True)
])

store_schema = StructType([
    StructField("store_id", StringType(), False),
    StructField("store_name", StringType(), True),
    StructField("location", StringType(), True)
])

sales_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("price", DoubleType(), True)
])


class Extractor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = get_logger("extractor")

    def load_csv(self, file_path: str, schema: StructType, header: bool = True) -> DataFrame:
        """Carga un CSV usando el esquema especificado."""
        return self.spark.read.csv(file_path, schema=schema, header=header)

    def clean_column_names(self, df: DataFrame) -> DataFrame:
        new_names = [c.strip().lower().replace(" ", "_") for c in df.columns]
        for old, new in zip(df.columns, new_names):
            df = df.withColumnRenamed(old, new)
        return df

    def check_missing_values(self, df: DataFrame) -> DataFrame:
        return df.select([col(c).isNull().cast("int").alias(c) for c in df.columns]) \
                 .groupBy().sum()

    def drop_duplicates(self, df: DataFrame) -> DataFrame:
        return df.dropDuplicates()

    def return_schema_file(self, file_path: str) -> StructType:
        if file_path == "data/input/products_uuid.csv":
            return product_schema
        elif file_path == "data/input/store_uuid.csv":
            return store_schema
        elif file_path == "data/input/sales_uuid.csv":
            return sales_schema
        else:
            raise ValueError(f"Schema not found for file: {file_path}")

    def run_extract_data_preparation(self, file_path: str) -> DataFrame:

        self.logger.info(f"Cargando: {file_path}")
        df = self.load_csv(file_path, self.return_schema_file(file_path))

        self.logger.info("Limpiando nombres de columnas...")
        df = self.clean_column_names(df)

        self.logger.info("Comprobando valores nulos...")
        nulls = self.check_missing_values(df)

        self.logger.info("Eliminando duplicados...")
        df = self.drop_duplicates(df)

        self.logger.info("Pipeline terminado.")
        return df
    
