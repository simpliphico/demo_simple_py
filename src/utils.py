from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import pandas as pd
import logging
import os
import yaml

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_file_to_df_spark(spark: SparkSession, path: str, filename: str, extension: str, schema: StructType):
    full_path = f"{path}/{filename}.{extension.lower()}"
    
    if extension.lower() == "csv":
        try:
            df = spark.read.option("header", True).schema(schema).csv(full_path)
            logging.info(f'Archivo {filename}.{extension} cargado correctamente de {path}.')
        except Exception as e:
            logging.error(f'Error al cargar el archivo CSV: {e}')
            raise
    else:
        logging.error(f"Extensión '{extension}' no soportada. Solo se permite 'csv'.")
        raise ValueError(f"Extensión '{extension}' no soportada. Solo se permite 'csv'.")
    
    return df


def load_file_to_df_pandas(filename: str, path: str, extension: str, columns=None, dtypes=None):
    """
    Carga un archivo en un DataFrame de pandas si es CSV. Permite especificar nombres de columnas y tipos de datos.
    """
    full_path = os.path.join(path, f"{filename}.{extension}")
    if extension.lower() == 'csv':
        try:
            df = pd.read_csv(full_path, names=columns, header=1 if columns is None else None, dtype=dtypes)
            logging.info(f'Archivo {filename}.{extension} cargado correctamente de {path}.')
            return df
        except Exception as e:
            logging.error(f'Error al cargar el archivo CSV: {e}')
            raise
    else:
        logging.error(f"Extensión '{extension}' no soportada. Solo se permite 'csv'.")
        raise ValueError(f"Extensión '{extension}' no soportada. Solo se permite 'csv'.")

def load_config(path="config/config.yml"):
    try:
        with open(path, "r") as f:
            config = yaml.safe_load(f)
            logging.info(f'Archivo de configuración {path} cargado correctamente.')
            return config
    except Exception as e:
        logging.error(f'Error al cargar el archivo de configuración {path}: {e}')
        raise


