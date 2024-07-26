from .utils import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import split, col
import os
from typing import Union
from pathlib import Path
import shutil

def load_dataset(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Loads data from a CSV file into a DataFrame.
    
    Args:
        spark (SparkSession): The SparkSession to use.
        file_path (str): Path to the CSV file.
        
    Returns:
        DataFrame: A DataFrame containing the loaded data.
    """
    logger.info(f"Loading data from {file_path}")
    df1 = spark.read.option("header", "true").csv(file_path, header=True, inferSchema=True, sep=',')
    return df1

# def write_data(df: DataFrame, output_dir: str = '../output_folder', single_file: bool = True) -> None:
#     """
#     writes a DataFrame into a CSV file.
    
#     Args:
#         df (DataFrame): The DataFrame to save.
#         output_dir (str): The directory to save the CSV file in. default value is set to the output folder
#         single_file (bool): Whether to save the DataFrame as a single file.
#     """
#     logger.info(f"Saving data to {output_dir}")
#     if single_file:
#         df.coalesce(1).write.csv(output_dir, header=True, mode='overwrite')
#     else:
#         df.write.csv(output_dir, header=True, mode='overwrite')

# def folder_exists(folder_path: Union[str, Path]) -> bool:
#     """
#     Check if a folder exists.

#     Args:
#         folder_path (Union[str, Path]): The path to the folder.

#     Returns:
#         bool: True if the folder exists, False otherwise.
#     """
#     folder_path = Path(folder_path)  # Convert to Path object if it's a string
#     return folder_path.exists() and folder_path.is_dir()

# def create_folder_if_not_exists(folder_path: Union[str, Path]) -> None:
#     """
#     Create a folder if it doesn't exist.

#     This function checks if the specified folder exists. If it doesn't,
#     it creates the folder. It uses the os module for folder operations
#     and logs the results.

#     Args:
#         folder_path (Union[str, Path]): The path to the folder to be created.

#     Returns:
#         None

#     Raises:
#         OSError: If there's an error creating the directory.
#     """
#     folder_path = Path(folder_path)  # Convert to Path object if it's a string

#     if not folder_exists(folder_path):
#         try:
#             os.makedirs(folder_path, exist_ok=True)
#             logger.info(f"Created folder: {folder_path}")
#         except OSError as e:
#             logger.error(f"Error creating folder {folder_path}: {e}")
#             raise
#     else:
#         logger.info(f"Folder already exists: {folder_path}")

    
def clear_folder_if_not_empty(folder_path: Union[str, Path]) -> None:
    """
    Clear the contents of a folder if it's not empty.

    This function first ensures the folder exists, then removes all its contents
    if it's not empty. It uses the os and shutil modules for file operations
    and logs the results.

    Args:
        folder_path (Union[str, Path]): The path to the folder to be cleared.

    Returns:
        None

    Raises:
        OSError: If there's an error during folder operations.
    """
    folder_path = Path(folder_path)  # Convert to Path object if it's a string

    try:
        # Ensure the folder exists
        folder_path.mkdir(parents=True, exist_ok=True)
        
        # Check if the folder is empty
        if any(folder_path.iterdir()):
            # Clear the contents of the folder
            for item in folder_path.iterdir():
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
            logger.info(f"Cleared contents of the folder: {folder_path}")
        else:
            logger.info(f"The folder is already empty: {folder_path}")
    
    except OSError as e:
        logger.error(f"Error during folder operations for {folder_path}: {e}")
        raise

def write_csv(df: DataFrame, folder_path: Union[str, Path], folder_name: str, file_name: str) -> None:
    """
    Write a PySpark DataFrame to a single CSV file.

    This function clears the target folder if it's not empty, writes the DataFrame
    to a temporary location, then moves the CSV file to the final location.

    Args:
        df (DataFrame): The PySpark DataFrame to write.
        folder_path (Union[str, Path]): The base folder path where the CSV file will be saved.
        folder_name (str): The name of the subfolder within the base folder.
        file_name (str): The name of the output CSV file.

    Returns:
        None

    Raises:
        OSError: If there's an error during file operations.
    """
    try:
        base_path = Path(folder_path)
        target_folder = base_path / folder_name
        temp_folder = target_folder / 'temp'
        final_file_path = target_folder / file_name

        # Ensure the target folder exists and is empty
        clear_folder_if_not_empty(target_folder)

        # Write DataFrame to temporary location
        df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(str(temp_folder))

        # Find the CSV file in the temporary folder
        csv_file = next(temp_folder.glob('*.csv'))

        # Move the CSV file to the final location
        shutil.move(str(csv_file), str(final_file_path))

        # Remove the temporary folder
        shutil.rmtree(temp_folder)

        logger.info(f"CSV file written successfully: {final_file_path}")

        # Show the DataFrame
        # df.show()

    except Exception as e:
        logger.error(f"Error writing CSV file: {e}")
        raise
