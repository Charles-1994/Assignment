from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, split, when, size, trim, desc
from .utils import logger
from .data_read_and_write import write_csv

def task1(spark: SparkSession, empDept: DataFrame, empInfo: DataFrame, output_folder: str, folder_name: str = 'it_data', file_name: str = 'it_data.csv') -> None:
    """
    Task 1: Process IT Data.
    
    Args:
        spark (SparkSession): The SparkSession object.
        empDept (DataFrame): The first dataset.
        empInfo (DataFrame): The second dataset.
        output_folder(str): The output folder.
        folder_name(str): The folder that needs to be created in the output folder
        fiile_name(str): The file that needs to be created in the requested folder
    """
    it_df = empDept.filter(col('area')=='IT')\
        .join(empInfo, on='id', how='left')\
        .sort(desc('sales_amount')).limit(100)
    
    write_csv(it_df, output_folder, folder_name, file_name)
    logger.info("Task 1: IT Data processed and saved successfully")

def task2(spark: SparkSession, empDept: DataFrame, empInfo: DataFrame, output_folder: str, folder_name: str = 'marketing_address_info', file_name: str = 'marketing_address_info.csv') -> None:
    """
    Task 1: Process Address of Marketing Area.
    
    Args:
        spark (SparkSession): The SparkSession object.
        empDept (DataFrame): The first dataset.
        empInfo (DataFrame): The second dataset.
        output_folder(str): The output folder.
        folder_name(str): The folder that needs to be created in the output folder
        fiile_name(str): The file that needs to be created in the requested folder
    """
    result_df = empDept.filter(col('area')=='Marketing').join(empInfo, on='id',how='left')
    result_df.select('name','address').display()

    # Create new columns based on the number of parts
    result_df = result_df.select('name','address')\
            .withColumn('split_col', split('address', ',')) \
            .withColumn('address_part1', when(size('split_col') == 3, col('split_col').getItem(0)).otherwise(None)) \
            .withColumn('address_part2', when(size('split_col') == 3, col('split_col').getItem(1)).otherwise(col('split_col').getItem(0))) \
            .withColumn('address_part3', when(size('split_col') == 3, col('split_col').getItem(2)).otherwise(col('split_col').getItem(1)))
    
    # Select the required columns
    result_df = result_df.select('name','address', 'address_part1', 'address_part2', 'address_part3')\
            .withColumn('zipcode',trim(col('address_part2')))\
            .withColumnRenamed('address_part3', 'city')
    
    marketing_address_info = result_df.select('address','zipcode')
    # marketing_address_info.display()

    write_csv(marketing_address_info, output_folder, folder_name, file_name)
    logger.info("Task 2: Addresses of Marketing Department are processed and saved successfully") 