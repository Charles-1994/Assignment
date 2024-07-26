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
    Task 2: Process Addresses of Marketing Area employees.
    
    Args:
        spark (SparkSession): The SparkSession object.
        empDept (DataFrame): The first dataset.
        empInfo (DataFrame): The second dataset.
        output_folder(str): The output folder.
        folder_name(str): The folder that needs to be created in the output folder
        fiile_name(str): The file that needs to be created in the requested folder
    """
    result_df = empDept.filter(col('area')=='Marketing').join(empInfo, on='id',how='left')
    # result_df.select('name','address').display()

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

def task3(spark: SparkSession, empDept: DataFrame, empInfo: DataFrame, output_folder: str, folder_name: str = 'department_breakdown', file_name: str = 'department_breakdown.csv') -> None:
    """
    Task 3: Process sales_amount and calls_successful_perc by Department.
    
    Args:
        spark (SparkSession): The SparkSession object.
        empDept (DataFrame): The first dataset.
        empInfo (DataFrame): The second dataset.
        output_folder(str): The output folder.
        folder_name(str): The folder that needs to be created in the output folder
        fiile_name(str): The file that needs to be created in the requested folder
    """
    # Creating temp views of the dfs
    empSales = empDept.join(empInfo, on = 'id', how='left')
    empSales.createOrReplaceTempView('empSales')

    sql_query = """
    select area, Format_number(round(sum(sales_amount),2),0) as sales_amount, sum(calls_made) as calls_made, 
        sum(calls_successful) as calls_successful,
        concat(round(sum(calls_successful)/sum(calls_made)*100,2),'%') as calls_successful_perc
    from empSales
    group by 1
    """
    department_data = spark.sql(sql_query)
    # department_data.show()

    write_csv(department_data.select('area','sales_amount','calls_successful_perc'), output_folder, folder_name, file_name)
    logger.info("Task 3: sales_amount and calls_successful_perc by Department are processed and saved successfully")

def task4(spark: SparkSession, empDept: DataFrame, empInfo: DataFrame, output_folder: str, folder_name: str = 'top_3', file_name: str = 'top_3.csv') -> None:
    """
    Task 4: Process top 3 performers in each area (sorted by calls_successful_perc, sales_amount in descending order).
    
    Args:
        spark (SparkSession): The SparkSession object.
        empDept (DataFrame): The first dataset.
        empInfo (DataFrame): The second dataset.
        output_folder(str): The output folder.
        folder_name(str): The folder that needs to be created in the output folder
        fiile_name(str): The file that needs to be created in the requested folder
    """

    empSales = empDept.join(empInfo, on = 'id', how='left')
    empSales.createOrReplaceTempView('empSales')

    sql_query = """
    with temp as (
    select area, name, format_number(sales_amount,0) as sales_amount, round(calls_successful/calls_made * 100,1) as calls_successful_perc
    from empSales
    )

    select area, name, sales_amount, rank_, concat(calls_successful_perc,'%') as calls_successful_perc
    from (
        select *, row_number() over (partition by area order by calls_successful_perc DESC, sales_amount DESC) as rank_
        from temp
    ) t
    where rank_ <= 3
    """
    top3_df = spark.sql(sql_query)
    # top3_df.display()    

    write_csv(top3_df, output_folder, folder_name, file_name)
    logger.info("Task 4: top 3 performers in each area (sorted by calls_successful_perc, sales_amount in descending order) are processed and saved successfully")

def task5(spark: SparkSession, empDept: DataFrame, clientsCalled: DataFrame, output_folder: str, folder_name: str = 'top_3_most_sold_per_department_netherlands', file_name: str = 'top_3_most_sold_per_department_netherlands.csv') -> None:
    """
    Task 5: Process top 3 most sold products per department in Netherlands.
    
    Args:
        spark (SparkSession): The SparkSession object.
        empDept (DataFrame): The first dataset.
        empInfo (DataFrame): The second dataset.
        output_folder(str): The output folder.
        folder_name(str): The folder that needs to be created in the output folder
        fiile_name(str): The file that needs to be created in the requested folder
    """

    clientsCalled.createOrReplaceTempView("clientsCalled")
    empDept.createOrReplaceTempView("empDept")

    sql_query = """
    with prd_table as (
        select ed.area, cc.product_sold, sum(cc.quantity) as prd_quantity from (
            select * from clientsCalled
            where country = 'Netherlands') cc
        left join empDept ed on cc.caller_id = ed.id
        group by 1,2
        order by area ASC, prd_quantity desc
    )

    select * from (
        select *, row_number() over (partition by area order by prd_quantity desc) as prd_rank
        from prd_table
    )
    where prd_rank <=3
    """

    top3_prd_NL = spark.sql(sql_query)
    # top3_prd_NL.show()

    write_csv(top3_prd_NL, output_folder, folder_name, file_name)
    logger.info("Task 5: top 3 most sold products per department in Netherlands are processed and saved successfully")

def task6(spark: SparkSession, empDept: DataFrame, empInfo: DataFrame, clientsCalled:DataFrame ,output_folder: str, folder_name: str = 'best_salesperson', file_name: str = 'best_salesperson.csv') -> None:
    """
    Task 6: Process Best Salesmen by country.
    
    Args:
        spark (SparkSession): The SparkSession object.
        empDept (DataFrame): The first dataset.
        empInfo (DataFrame): The second dataset.
        output_folder(str): The output folder.
        folder_name(str): The folder that needs to be created in the output folder
        fiile_name(str): The file that needs to be created in the requested folder
    """

    empSales = empDept.join(empInfo, on = 'id', how='left')
    empSales.createOrReplaceTempView('empSales')

    clientsCalled.createOrReplaceTempView('clientsCalled')
    sql_query = """
    with countryWide as (
    select country, caller_id, sum(quantity) as quantity from clientsCalled
    group by 1,2
    )

    select cw.country, es.id, es.name, es.area, cw.quantity from (
        select *, row_number() over (partition by country order by quantity desc) as rank_1 from countryWide) cw
    left join empSales es on cw.caller_id = es.id
    where cw.rank_1 = 1
    order by cw.country
    """

    best_salesperson = spark.sql(sql_query)
    # best_salesperson.show()

    write_csv(best_salesperson, output_folder, folder_name, file_name)
    logger.info("Task 6: Best Salesmen by country are processed and saved successfully")