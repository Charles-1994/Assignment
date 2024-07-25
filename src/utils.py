import logging
from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession

def get_spark_session(app_name: str ='SalesData') -> SparkSession:
    """
    Creates a SparkSession.
    
    Args:
        app_name (str): Name of the Spark application.
        
    Returns:
        SparkSession: A SparkSession object.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

# Configure logging
def setup_logger() -> logging.Logger:
    """
    Setup and return a logger with a rotating file handler.
    
    Returns:
        Logger: Configured logger.
    """
    logger = logging.getLogger("SalesDataLogger")
    logger.setLevel(logging.INFO)
    log_file = "sales_data.log"

    # Create handlers
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    file_handler = RotatingFileHandler(log_file, maxBytes=1*1024*1024, backupCount=5)
    file_handler.setLevel(logging.INFO)

    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

logger = setup_logger()