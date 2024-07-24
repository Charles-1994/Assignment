from pyspark.sql import SparkSession, DataFrame
# from typing import Optional
# import logging

# # Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

def create_spark_session() -> SparkSession:
    """
    Create and return a SparkSession.

    Returns:
        SparkSession: The created SparkSession object.
    """
    return SparkSession.builder.appName("SalesAnalysis").getOrCreate()

if __name__ == "__main__":
    # This block is for testing purposes
    spark = create_spark_session()
    # test_df = load_dataset(spark, "path/to/test/file.csv")

    test_df = spark.read.format("csv").load("./Source_Datasets/dataset_one.csv", header=True, inferSchema=True, sep=",")

    test_df.show()
    spark.stop()