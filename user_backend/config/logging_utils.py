import logging
import sys

def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Configures and returns a logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Create a handler
    handler = logging.StreamHandler(sys.stdout) # Log to stdout
    handler.setLevel(level)

    # Create a formatter and add it to the handler
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # Add the handler to the logger
    # Check if handlers are already added to avoid duplication if get_logger is called multiple times for the same logger name
    if not logger.handlers:
        logger.addHandler(handler)

    return logger

# Example of a default logger if needed directly
# default_logger = get_logger("app_default")