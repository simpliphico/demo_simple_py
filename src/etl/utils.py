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
    Configures and returns a custom logger for the application.

    Args:
        name: Logger name
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Custom format for messages
        log_to_file: Whether to also write to file
        log_file: Log file name
    Returns:
        Configured logger
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid duplicating handlers if already exists
    if logger.handlers:
        return logger
    # Default format
    if log_format is None:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format)
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    # File handler (optional)
    if log_to_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger


def get_logger(name: str = "demo_simple_py") -> logging.Logger:
    """
    Gets an already configured logger or creates a new one if it doesn't exist.

    Args:
        name: Logger name

    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)

    # If logger has no handlers, configure it
    if not logger.handlers:
        logger = setup_logger(name)
    return logger
