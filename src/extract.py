from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from utils import load_file_to_df_spark,load_file_to_df_pandas
import logging

def init_spark(app_name="DataPipeline"):
    try:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        logging.info(f'SparkSession inicializada con el nombre de aplicación: {app_name}')
        return spark
    except Exception as e:
        logging.error(f'Error al inicializar SparkSession: {e}')
        raise

def load_products(spark: SparkSession, path: str):
    schema = StructType() \
        .add("product_id", StringType()) \
        .add("product_name", StringType()) \
        .add("category", StringType())
    try:
        df = load_file_to_df_spark(spark, path, "products_uuid", "csv", schema)
        logging.info('Archivo products_uuid.csv cargado correctamente con Spark.')
        return df
    except Exception as e:
        logging.error(f'Error al cargar products_uuid.csv con Spark: {e}')
        raise

def load_stores(spark: SparkSession, path: str):
    schema = StructType() \
        .add("store_id", StringType()) \
        .add("store_name", StringType()) \
        .add("location", IntegerType())
    try:
        df = load_file_to_df_spark(spark, path, "stores_uuid", "csv", schema)
        logging.info('Archivo stores_uuid.csv cargado correctamente con Spark.')
        return df
    except Exception as e:
        logging.error(f'Error al cargar stores_uuid.csv con Spark: {e}')
        raise

def load_sales(spark: SparkSession, path: str):
    schema = StructType() \
        .add("transaction_id", StringType()) \
        .add("store_id", StringType()) \
        .add("product_id", StringType())\
        .add("quantity", IntegerType())\
        .add("transaction_date", StringType())\
        .add("price", DoubleType())
    try:
        df = load_file_to_df_spark(spark, path, "sales_uuid", "csv", schema)
        logging.info('Archivo sales_uuid.csv cargado correctamente con Spark.')
        return df
    except Exception as e:
        logging.error(f'Error al cargar sales_uuid.csv con Spark: {e}')
        raise

def load_products_df(path: str):
    columns = ['product_id', 'product_name', 'category']
    dtypes = {'id': str, 'name': str, 'category': str}
    try:
        df = load_file_to_df_pandas('products_uuid', path, 'csv', columns=columns, dtypes=dtypes)
        logging.info('Archivo products_uuid.csv cargado correctamente con pandas.')
        return df
    except Exception as e:
        logging.error(f'Error al cargar products_uuid.csv con pandas: {e}')
        raise

def load_sales_df(path: str):
    columns = ['transaction_id', 'store_id', 'product_id', 'quantity', 'transaction_date', 'price']
    dtypes = {
        'transaction_id': str,
        'store_id': str,
        'product_id': str,
        'quantity': str,
        'transaction_date': str,
        'price': str
    }
    try:
        df = load_file_to_df_pandas('sales_uuid', path, 'csv', columns=columns, dtypes=dtypes)
        logging.info('Archivo sales_uuid.csv cargado correctamente con pandas.')
        return df
    except Exception as e:
        logging.error(f'Error al cargar sales_uuid.csv con pandas: {e}')
        raise

def load_stores_df(path: str):
    columns = ['store_id', 'store_name', 'location']
    dtypes = {'store_id': str, 'store_name': str, 'location': str}
    try:
        df = load_file_to_df_pandas('stores_uuid', path, 'csv', columns=columns, dtypes=dtypes)
        logging.info('Archivo stores_uuid.csv cargado correctamente con pandas.')
        return df
    except Exception as e:
        logging.error(f'Error al cargar stores_uuid.csv con pandas: {e}')
        raise