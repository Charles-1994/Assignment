# ABN Amro Assignment: Sales-Data Project

## Overview
This project processes and analyzes sales data using PySpark.

## Structure
- **src/**: Source code for all the tasks asked in the exercise.md
- **tests/**: Test cases for the source code.
- **Source_Datasets/**: Sample data files.
- **.github/**: GitHub Actions workflow for CI/CD.
- **requirements.txt**: Python dependencies.
- **README.md**: Project documentation.
- **setup.py**: Project source distribution file
- **.pre-commit-config.yaml**: config file for the pre commit
- **exercise.md**: Tasks instructions


### Running the Project
1. **Install the package**:
   ```bash
   pip install .
   ```

2. **Run the application**
    ```bash
    sales-data
    ```

3. **Run the tests**
    ```bash
    pytest tests/
    ```

### Notes:
This project's main objective is to read the data sets from the Source_Datasets. each data set is read empDept, empInfo and clientsCalled spark data frames respective.

### src:
- **src/data_read_and_write.py**: contains code to two main functions
    - to load datasets
    - write data frames into csv (this used two other functions to check if a folder is empty or not and create folder)
- **src/tasks.py**: contains functioins to perform all the tasks fromm 1 - 6 and two more additional tasks
- **src/utils.py**: this file intializes sparkSession and logger
- **src/main.py**: contains all the calls to tasks and spark session

### tests: 
test functions are implemented using chispa module functions
- **tests/test_data_read_and_write.py**: test functions to test data_read_and_write.py (Data frame and schema comparison tests 
- **tests/test_tasks.py**: test functions to test all the task from 1 to 6

### .githu/workflows:
there are two jobs setup
1. linter.yaml: to perform code quality linting operations using pylint, mypy and other pre-commit hooks
2. python-app.yaml: to install dependencies, run tests, build and package the application in to artifact

Files that are generated when the application is run
1. **`Output_folder/*`**: all the csv files of the tasks are generated as per the requested instructions
2. **sales-data.log**: the log run by rotation policy

## Task Details:
### Output #1 - **IT Data**
The management teams wants some specific information about the people that are working in selling IT products.
- Join the two datasets.
- Filter the data on the **IT** department.
- Order the data by the sales amount, biggest should come first.
- Save only the first **100** records.
- The output directory should be called **it_data** and you must use PySpark to save only to one **CSV** file.
**_Note_**: Filter IT department first and joined the table. Efficient way to join tables with lesser records.

### Output #2 - **Marketing Address Information**
The management team wants to send some presents to team members that are work only selling **Marketing** products and wants a list of only addresses and zip code, but the zip code needs to be in it's own column.
- The output directory should be called **marketing_address_info** and you must use PySpark to save only to one **CSV** file.
**_Note_**:
Assuming we are sending presents to all the people who belong to Marketing Dept
Since there are only 198 employees and all the address columns have proper Zip code format. We can
- either split the address column and based on the size of the list, divide columns in to 3 new columns, where we extract zip code
- or Using regex to find the regex_expr for NL zipcode and get a new column. Regex is compute intensive

### Output #3 - **Department Breakdown**
The stakeholders want to have a breakdown of the sales amount of each department and they also want to see the total percentage of calls_succesfful/calls_made per department. The amount of money and percentage should be easily readable.
- The output directory should be called **department_breakdown** and you must use PySpark to save only to one **CSV** file.
- 
### Output #4 - **Top 3 best performers per department**
The management team wants to reward it's best employees with a bonus and therefore it wants to know the name of the top 3 best performers per department. That is the ones that have a percentage of calls_succesfful/calls_made higher than 75%. It also wants to know the sales amount of these employees to see who best deserves the bonus. In your opinion, who should get it and why?
- The output directory should be called **top_3** and you must use PySpark to save only to one **CSV** file.
**_Note_**:
In the above table, we have the top 3 performers in each area (sorted by calls_successful_perc, sales_amount in descending order)
Even though for some personnel, calls_successful_perc is greater, their sales_amount is less. a difference of 2% in calls_successful_perc does not make up 5000 euros difference in sales_amount. According to me, the winners should be
Finance - Sanne Verbeeck
Games - Sven Lutterveld
HR - Jayda Wilcken
IT - Puck Tins
Marketing - Lara van Vlaanderen-van de Darnau

### Output #5 - **Top 3 most sold products per department in the Netherlands**
- The output directory should be called **top_3_most_sold_per_department_netherlands** and you must use PySpark to save only to one **CSV** file.
**_Note_**: top 3 products by department

### Output #6 - **Who is the best overall salesperson per country**
- The output directory should be called **best_salesperson** and you must use PySpark to save only to one **CSV** file.
**_Note_**: 
In both empDept and empInfo data sets, we dont have any info about country specifi sales or calls_made. to determine the best salesmen by country we can only take the quantity of products sold as the metrics to determine the best salesmen.

## What is wanted? Extra Bonus
- Please derive other two insights from the three datasets that you find interesting. Either create new datasets in **CSV** or if you prefer create some graphs.
- Please save them as **extra_insight_one** and **extra_insight_two** directories and if you create a dataset you must use PySpark to save only to one **CSV** file.

1.	**extra_insight_one**: The above are some of the KPI's which can help us compare the departments as a whole.
_Observations_: Tried to observe the relation between the successful calls made with the sales_amount. 
   1. But there is no strong correlation between the calls_success_rate and sales_amount.
   2. The best performing department in terms of sales is Games

2.	**extra_insight_two**: This data gives us the least performers of the department based on Sales amount ranking and calls_success_rate ranking

