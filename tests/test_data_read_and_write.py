import pytest 
from pyspark.sql import SparkSession
from chispa import assert_df_equality
from src.data_read_and_write import load_dataset, write_data, write_csv
from src.utils import get_spark_session
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pathlib import Path

@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return get_spark_session()

def test_load_dataset(spark: SparkSession) -> None:
    df = load_dataset(spark, "./Source_Datasets/dataset_one.csv")
    assert df is not None

def test_write_data(spark: SparkSession, tmp_path:Path) -> None:
    data = [("John Doe", 1000)]
    # Define the schema
    schema = StructType([
        StructField("name", StringType(), nullable=True),
        StructField("sales_amount", IntegerType(), nullable=True)
    ])
    df = spark.createDataFrame(data, schema)
    output_path = tmp_path / "output.csv"
    write_data(df, str(output_path))
    written_df = spark.read.csv(str(output_path), header=True, inferSchema=True)
    assert_df_equality(df, written_df)

def test_write_csv(spark: SparkSession, tmp_path: Path) -> None:
    data = [("John Doe", 1000)]
    # Define the schema
    schema = StructType([
            StructField("name", StringType(), nullable=True),
            StructField("sales_amount", IntegerType(), nullable=True)
    ])
    df = spark.createDataFrame(data, schema)

    folder_name = "test_folder"
    file_name = "output.csv"
    folder_path = tmp_path / "output_folder"

    # Read the written CSV file
    output_path = folder_path / folder_name / file_name
    print(str(output_path))

    # Call write_csv function
    write_csv(df, str(folder_path), folder_name, file_name)

    print(f"Expected output path: {output_path}")
    assert output_path.exists(), f"CSV file was not created at {output_path}"

    written_df =  spark.read.option("header", "true").csv(str(output_path), header=True, schema=schema, sep=',')

    # # Assert equality of original and written DataFrames
    assert_df_equality(df, written_df)