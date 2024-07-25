import sys
from .utils import get_spark_session, logger
from .data_read_and_write import load_dataset, write_data, write_csv
from .tasks import task1
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
    task1(spark, empDept, empInfo, output_folder)

    # folder_name = 'it_data'
    # file_name = 'it_data.csv'
    # write_csv(it_df, output_folder, folder_name, file_name)
    
if __name__ == "__main__":
    if len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        logger.warning("Using default dataset paths.")
        main()