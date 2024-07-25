from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import desc, col
from .utils import logger
from .data_read_and_write import write_csv

output_folder = "./output_folder/"

def task1(spark: SparkSession, empDept: DataFrame, empInfo: DataFrame) -> None:
    """
    Task 1: Process IT Data.
    
    Args:
        spark (SparkSession): The SparkSession object.
        df1 (DataFrame): The first dataset.
        df2 (DataFrame): The second dataset.
    """
    it_df = empDept.filter(col('area')=='IT')\
        .join(empInfo, on='id', how='left')\
        .sort(desc('sales_amount')).limit(100)
    folder_name = 'it_data'
    file_name = 'it_data.csv'
    write_csv(it_df, output_folder, folder_name, file_name)
