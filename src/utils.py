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
    handler = RotatingFileHandler("sales_data.log", maxBytes=1000000, backupCount=3)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

logger = setup_logger()