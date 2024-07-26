import sys
from .utils import get_spark_session, logger
from .data_read_and_write import load_dataset, write_data, write_csv
from .tasks import task1, task2, task3, task4, task5, task6
# from .analysis import filter_it_data, get_top_100_it_sales, get_marketing_address_info, department_breakdown

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
    
    spark = get_spark_session()

    empDept = load_dataset(spark, dataset_one_path)
    empInfo = load_dataset(spark, dataset_two_path)
    clientsCalled = load_dataset(spark, dataset_three_path)

    output_folder = "./output_folder/"

    # Task 1
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

    
if __name__ == "__main__":
    if len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        logger.warning("Using default dataset paths.")
        main()