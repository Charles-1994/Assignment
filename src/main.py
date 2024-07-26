import sys
from .utils import get_spark_session, logger
from pyspark.sql import SparkSession
from .data_read_and_write import load_dataset
from .tasks import task1, task2, task3, task4, task5, task6, extraTask1, extraTask2

def main(
    dataset_one_path: str = "./Source_Datasets/dataset_one.csv",
    dataset_two_path: str = "./Source_Datasets/dataset_two.csv",
    dataset_three_path: str = "./Source_Datasets/dataset_three.csv"
) -> None:
    """
    Main function to execute the required data processing and analysis tasks.
    
    Args:
        dataset_one_path (str): Path to the first dataset (dataset_one.csv)
        dataset_two_path (str): Path to the second dataset (dataset_two.csv)
        dataset_three_path (str): Path to the third dataset (dataset_three.csv)
    """
    
    spark:SparkSession = get_spark_session()

    empDept = load_dataset(spark, dataset_one_path)
    empInfo = load_dataset(spark, dataset_two_path)
    clientsCalled = load_dataset(spark, dataset_three_path)

    output_folder = "./output_folder/"

    # calling all task functions
    logger.info("Executing Task 1: Processing IT Data")
    task1(spark, empDept, empInfo, output_folder)

    logger.info("Executing Task 2: Addresses of Marketing Area employees")
    task2(spark, empDept, empInfo, output_folder)

    logger.info("Executing Task 3: Processing sales_amount and calls_successful_perc by Department")
    task3(spark, empDept, empInfo, output_folder)

    logger.info("Executing Task 4: Processing top 3 performers in each area (sorted by calls_successful_perc, sales_amount in descending order)")
    task4(spark, empDept, empInfo, output_folder)

    logger.info("Executing Task 5: Processing top 3 most sold products per department in Netherlands")
    task5(spark, empDept, clientsCalled, output_folder)

    logger.info("Executing Task 6: Processing Best Salesmen by country")
    task6(spark, empDept, empInfo, clientsCalled, output_folder)

    logger.info("Executing extra Task1: Processing KPI's that can  help compare Departments")
    extraTask1(spark, empDept, empInfo, output_folder)

    logger.info("Executing extra Task2: Processing least 5 performers by department (based on calls_success_rate vs sales_amount)")
    extraTask2(spark, empDept, empInfo, output_folder)

    
if __name__ == "__main__":
    if len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        logger.warning("Using default dataset paths.")
        main()