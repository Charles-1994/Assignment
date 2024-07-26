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
