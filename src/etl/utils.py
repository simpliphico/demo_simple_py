import logging
import sys
from typing import Optional


def setup_logger(
    name: str = "demo_simple_py",
    level: int = logging.INFO,
    log_format: Optional[str] = None,
    log_to_file: bool = False,
    log_file: str = "app.log"
) -> logging.Logger:
    """
    Configura y retorna un logger personalizado para la aplicación.
    
    Args:
        name: Nombre del logger
        level: Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Formato personalizado para los mensajes
        log_to_file: Si se debe escribir también a archivo
        log_file: Nombre del archivo de log
    
    Returns:
        Logger configurado
    """
    # Crear el logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Evitar duplicar handlers si ya existe
    if logger.handlers:
        return logger
    
    # Formato por defecto
    if log_format is None:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    formatter = logging.Formatter(log_format)
    
    # Handler para consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Handler para archivo (opcional)
    if log_to_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str = "demo_simple_py") -> logging.Logger:
    """
    Obtiene un logger ya configurado o crea uno nuevo si no existe.
    
    Args:
        name: Nombre del logger
    
    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)
    
    # Si el logger no tiene handlers, configurarlo
    if not logger.handlers:
        logger = setup_logger(name)
    
    return logger
