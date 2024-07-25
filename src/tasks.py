from pyspark.sql.functions import desc, col
from .data_read_and_write import write_csv

output_folder = "../output_folder/"

def Task1(empDept,empInfo):
    it_df = empDept.filter(col('area')=='IT')\
        .join(empInfo, on='id', how='left')\
        .sort(desc('sales_amount')).limit(100)
    folder_name = 'it_data'
    file_name = 'it_data.csv'
    write_csv(it_df, output_folder, folder_name, file_name)
