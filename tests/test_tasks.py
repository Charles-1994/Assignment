import pytest
from pyspark.sql import SparkSession, DataFrame
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.functions import col, desc
from src.tasks import task1
from src.data_read_and_write import load_dataset

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
    schema = ["id", "area", "calls_made", "calls_successful"]
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
    schema = ["id", "name", "address", "sales_amount"]
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
    schema = ["id", "caller_id", "company", "recipient", "age",	"country", "product_sold",	"quantity"]

    return spark.createDataFrame(data, schema)

def test_task1(spark: SparkSession, emp_dept_df: DataFrame, emp_info_df: DataFrame, tmp_path) -> None:
    """
    Test for task1 function to ensure it processes IT data correctly.
    
    Args:
        spark (SparkSession): The Spark session object.
        emp_dept_df (DataFrame): Sample employee department DataFrame.
        emp_info_df (DataFrame): Sample employee information DataFrame.
        tmp_path: Temporary path for writing output.
    """
    output_folder = str(tmp_path)
    folder_name = 'it_data'
    file_name = 'it_data.csv'
    expected_data = [
        (2, "IT", 60, 20, "Jane Smith", "Lindehof 5, 4133 HB, Nederhemert", 2000),
        (1, "IT", 50, 10, "John Doe", "2588 VD, Kropswolde", 1000)
    ]
    expected_schema = ["id", "area", "calls_made", "calls_successful", "name", "address", "sales_amount"]
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    task1(spark, emp_dept_df, emp_info_df)
    
    output_path = f"{output_folder}/{folder_name}/{file_name}"
    result_df = load_dataset(spark, output_path)
    
    assert_df_equality(result_df, expected_df)
