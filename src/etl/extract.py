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
        "filename": "store_uuid.csv",
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
        """Carga un CSV usando el esquema especificado."""
        return self.spark.read.csv(file_path, header=header)

    def get_metadata_file(self, filename: str) -> dict:
        for file in files_metadata:
            if file["filename"] == filename:
                return file
        raise ValueError(f"Archivo no encontrado: {filename}")

    def format_date(self, column_name: Column) -> Column:
        formats_list = [
            "yyyy/MM/dd", "yy/MM/dd", "yyyy-MM-dd",
            "MM/dd/yyyy", "dd-MM-yyyy", "MM-dd-yyyy", "MMMM dd, yyyy"
            ]
        try:
            return coalesce(*[to_date(column_name, format) for format in formats_list])
        except Exception as e:
            raise ValueError(
                f"Formato de fecha no válido para la columna {column_name}: {str(e)}"
                )

    def validate_and_clean_columns(self, df: DataFrame, filename: str) -> DataFrame:
        file_info = self.get_metadata_file(filename)
        for column in file_info["columns"]:
            if column["name"] not in df.columns:
                raise ValueError(
                    f"Columna {column['name']} no encontrada en el archivo"
                    )
            if column["type"] == "date":
                df = df.withColumn(column["name"], self.format_date(column["name"]))
            elif column["type"] == "integer":
                df = df.withColumn(column["name"], col(column["name"]).cast("integer"))
            elif column["type"] == "double":
                df = df.withColumn(column["name"], col(column["name"]).cast("double"))
            else:
                self.logger.info(
                    f"No es necesario transformar la columna {column['name']}"
                    )
        return df

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

    def generate_path(self, filename: str, folder: str) -> str:
        """Devuelve la ruta completa del fichero combinando carpeta y nombre."""
        path = os.path.normpath(os.path.join(folder, filename))
        return path

    def run_extract_data_preparation(self, filename: str, folder: str) -> DataFrame:

        file_path = self.generate_path(filename, folder)

        self.logger.info(f"Cargando: {file_path}")
        df = self.load_csv(file_path)

        self.logger.info("Limpiando nombres de columnas...")
        df = self.clean_column_names(df)

        self.logger.info("Validando y limpiando columnas...")
        df = self.validate_and_clean_columns(df, filename)

        self.logger.info("Comprobando valores nulos...")
        nulls = self.check_missing_values(df)
        nulls.show()

        self.logger.info("Eliminando duplicados...")
        df = self.drop_duplicates(df)

        self.logger.info("Pipeline terminado.")
        return df
