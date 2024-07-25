import logging
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
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)