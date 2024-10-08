import pytest
import pkg_resources
from pyspark.sql import SparkSession, DataFrame
from chispa.dataframe_comparer import assert_df_equality
from chispa.schema_comparer import assert_schema_equality
from src.tasks import task1, task2, task3, task4, task5, task6
from src.data_read_and_write import load_dataset
from pathlib import Path
import shutil
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """
    Fixture to initialize a Spark session.
    
    Returns:
        SparkSession: The Spark session object.
    """
    return SparkSession.builder.appName("Test").getOrCreate()

@pytest.fixture(scope="module")
def emp_dept_df(spark: SparkSession) -> DataFrame:
    """
    Fixture to create a sample employee department DataFrame.
    
    Args:
        spark (SparkSession): The Spark session object.
    
    Returns:
        DataFrame: Sample employee department DataFrame.
    """
    data = [
        (1, "IT", 50, 10),
        (2, "IT", 60, 20),
        (3, "Marketing", 40, 30)
    ]
    # schema = ["id", "area", "calls_made", "calls_successful"]
    schema = StructType([
        StructField("id", IntegerType(), nullable=True),
        StructField("area", StringType(), nullable=True),
        StructField("calls_made", IntegerType(), nullable=True),
        StructField("calls_successful", IntegerType(), nullable=True)])
    return spark.createDataFrame(data, schema)

@pytest.fixture(scope="module")
def emp_info_df(spark: SparkSession) -> DataFrame:
    """
    Fixture to create a sample employee information DataFrame.
    
    Args:
        spark (SparkSession): The Spark session object.
    
    Returns:
        DataFrame: Sample employee information DataFrame.
    """
    data = [
        (1, "John Doe", "2588 VD, Kropswolde", 1000),
        (2, "Jane Smith", "Lindehof 5, 4133 HB, Nederhemert", 2000),
        (3, "Jim Brown", "Thijmenweg 38, 7801 OC, Grijpskerk", 3000)
    ]
    # schema = ["id", "name", "address", "sales_amount"]
    schema = StructType([
        StructField("id", IntegerType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("address", StringType(), nullable=True),
        StructField("sales_amount", IntegerType(), nullable=True)])
    return spark.createDataFrame(data, schema)

@pytest.fixture(scope="module")
def clientsCalled_df(spark: SparkSession) -> DataFrame:
    """
    Fixture to create a sample client called DataFrame.
    
    Args:
        spark (SparkSession): The Spark session object.
    
    Returns:
        DataFrame: Sample client called DataFrame.
    """
    data = [
        (1,	40,	"Verbruggen-Vermeulen CommV", "Anny Claessens", 45, "Belgium", "Banner", 50),
        (2,	17,	"Hendrickx CV",	"Lutgarde Van Loock", 41, "Belgium", "Sign", 23)
    ]
    # schema = ["id", "caller_id", "company", "recipient", "age",	"country", "product_sold",	"quantity"]
    schema = StructType([
        StructField("id", IntegerType(), nullable=True),
        StructField("caller_id", IntegerType(), nullable=True),
        StructField("company", StringType(), nullable=True),
        StructField("recipient", StringType(), nullable=True),
        StructField("age", IntegerType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("product_sold", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True)
    ])

    return spark.createDataFrame(data, schema)

def test_task1(spark: SparkSession, emp_dept_df: DataFrame, emp_info_df: DataFrame, tmp_path: Path) -> None:
    """
    Test for task1 function to ensure it processes IT data correctly.
    
    Args:
        spark (SparkSession): The Spark session object.
        emp_dept_df (DataFrame): Sample employee department DataFrame.
        emp_info_df (DataFrame): Sample employee information DataFrame.
        tmp_path: Temporary path for writing output.
    """
    output_folder = tmp_path
    folder_name = 'it_data'
    file_name = 'it_data.csv'
    expected_data = [
        (2, "IT", 60, 20, "Jane Smith", "Lindehof 5, 4133 HB, Nederhemert", 2000),
        (1, "IT", 50, 10, "John Doe", "2588 VD, Kropswolde", 1000)
    ]
    expected_schema = StructType([
        StructField("id", IntegerType(), nullable=True),
        StructField("area", StringType(), nullable=True),
        StructField("calls_made", IntegerType(), nullable=True),
        StructField("calls_successful", IntegerType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("address", StringType(), nullable=True),
        StructField("sales_amount", IntegerType(), nullable=True)])
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    # Ensure the directory is clean
    test_output_path = tmp_path / folder_name
    if test_output_path.exists():
        shutil.rmtree(test_output_path)
    
    task1(spark, emp_dept_df, emp_info_df, output_folder, folder_name, file_name)
    
    output_path = output_folder/folder_name/file_name
    result_df = load_dataset(spark, str(output_path))
    
    assert_df_equality(result_df, expected_df)


def test_task2(spark: SparkSession, emp_dept_df: DataFrame, emp_info_df: DataFrame, tmp_path: Path) -> None:
    """
    Test for task2 function to ensure it processes Addresses of Marketing Department correctly.
    
    Args:
        spark (SparkSession): The Spark session object.
        emp_dept_df (DataFrame): Sample employee department DataFrame.
        emp_info_df (DataFrame): Sample employee information DataFrame.
        tmp_path: Temporary path for writing output.
    """

    output_folder = tmp_path
    folder_name = 'marketing_address_info'
    file_name = 'marketing_address_info.csv'
    # expected_data = [
    #     ("Lindehof 5, 4133 HB, Nederhemert", "4133 HB"),
    #     ("2588 VD, Kropswolde", "2588 VD"),
    #     ("Thijmenweg 38", "7801 OC")
    # ]
    expected_schema = StructType([
        StructField("address", StringType(), nullable=True),
        StructField("zipcode", StringType(), nullable=True)
  ])
    # expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    # Ensure the directory is clean
    test_output_path = tmp_path / folder_name
    if test_output_path.exists():
        shutil.rmtree(test_output_path)
    
    task2(spark, emp_dept_df, emp_info_df, output_folder, folder_name, file_name)
    
    output_path = output_folder/folder_name/file_name
    result_df = load_dataset(spark, str(output_path))
    
    assert_schema_equality(result_df.schema, expected_schema)

def test_task3(spark: SparkSession, emp_dept_df: DataFrame, emp_info_df: DataFrame, tmp_path: Path) -> None:
    """
    Test for task3 function to ensure it processes sales_amount and calls_successful_perc by Department.

    Args:
        spark (SparkSession): The Spark session object.
        emp_dept_df (DataFrame): Sample employee department DataFrame.
        emp_info_df (DataFrame): Sample employee information DataFrame.
        tmp_path: Temporary path for writing output.
    """

    output_folder = tmp_path
    folder_name = 'department_breakdown'
    file_name = 'department_breakdown.csv'
    expected_schema = StructType([
        StructField("area", StringType(), nullable=True),
        StructField("sales_amount", StringType(), nullable=True),
        StructField("calls_successful_perc", StringType(), nullable=True)
  ])
    # expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    # Ensure the directory is clean
    test_output_path = tmp_path / folder_name
    if test_output_path.exists():
        shutil.rmtree(test_output_path)
    
    task3(spark, emp_dept_df, emp_info_df, output_folder, folder_name, file_name)
    
    output_path = output_folder/folder_name/file_name
    result_df = load_dataset(spark, str(output_path))
    
    assert_schema_equality(result_df.schema, expected_schema)

def test_task4(spark: SparkSession, emp_dept_df: DataFrame, emp_info_df: DataFrame, tmp_path: Path) -> None:
    """
    Test for task4 function to ensure it processes top 3 performers in each area (sorted by calls_successful_perc, sales_amount in descending order).
    Args:
        spark (SparkSession): The Spark session object.
        emp_dept_df (DataFrame): Sample employee department DataFrame.
        emp_info_df (DataFrame): Sample employee information DataFrame.
        tmp_path: Temporary path for writing output.
    """

    output_folder = tmp_path
    folder_name = 'top_3'
    file_name = 'top_3.csv'
    expected_schema = StructType([
        StructField("area", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),        
        StructField("sales_amount", StringType(), nullable=True),
        StructField("rank_", IntegerType(), nullable=True),
        StructField("calls_successful_perc", StringType(), nullable=True)
  ])
    # expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    # Ensure the directory is clean
    test_output_path = tmp_path / folder_name
    if test_output_path.exists():
        shutil.rmtree(test_output_path)
    
    task4(spark, emp_dept_df, emp_info_df, output_folder, folder_name, file_name)
    
    output_path = output_folder/folder_name/file_name
    result_df = load_dataset(spark, str(output_path))
    
    assert_schema_equality(result_df.schema, expected_schema)

def test_task5(spark: SparkSession, emp_dept_df: DataFrame, clientsCalled_df: DataFrame, tmp_path: Path) -> None:
    """
    Test for task5 function to ensure it processes top 3 most sold products per department in Netherlands.

    Args:
        spark (SparkSession): The Spark session object.
        emp_dept_df (DataFrame): Sample employee department DataFrame.
        emp_info_df (DataFrame): Sample employee information DataFrame.
        tmp_path: Temporary path for writing output.
    """

    output_folder = tmp_path
    folder_name = 'top_3_most_sold_per_department_netherlands'
    file_name = 'top_3_most_sold_per_department_netherlands.csv'
    expected_schema = StructType([
        StructField("area", StringType(), nullable=True),
        StructField("product_sold", StringType(), nullable=True),
        StructField("prd_quantity", StringType(), nullable=True),
        StructField("prd_rank", StringType(), nullable=True)
  ])
    # expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    # Ensure the directory is clean
    test_output_path = tmp_path / folder_name
    if test_output_path.exists():
        shutil.rmtree(test_output_path)
    
    task5(spark, emp_dept_df, clientsCalled_df, output_folder, folder_name, file_name)
    
    output_path = output_folder/folder_name/file_name
    result_df = load_dataset(spark, str(output_path))
    
    assert_schema_equality(result_df.schema, expected_schema)

def test_task6(spark: SparkSession, emp_dept_df: DataFrame, emp_info_df: DataFrame, clientsCalled_df: DataFrame, tmp_path: Path) -> None:
    """
    Test for task5 function to ensure it processes  Best Salesmen by country.

    Args:
        spark (SparkSession): The Spark session object.
        emp_dept_df (DataFrame): Sample employee department DataFrame.
        emp_info_df (DataFrame): Sample employee information DataFrame.
        tmp_path: Temporary path for writing output.
    """

    output_folder = tmp_path
    folder_name = 'best_salesperson'
    file_name = 'best_salesperson.csv'
    expected_schema = StructType([
        StructField("country", StringType(), nullable=True),
        StructField("id", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("area", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True)
  ])
    # expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    # Ensure the directory is clean
    test_output_path = tmp_path / folder_name
    if test_output_path.exists():
        shutil.rmtree(test_output_path)
    
    task6(spark, emp_dept_df, emp_info_df, clientsCalled_df, output_folder, folder_name, file_name)
    
    output_path = output_folder/folder_name/file_name
    result_df = load_dataset(spark, str(output_path))
    
    assert_schema_equality(result_df.schema, expected_schema)