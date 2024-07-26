from setuptools import setup, find_packages  # type: ignore
from typing import List

def parse_requirements(filename: str) -> List[str]:
    """
    Load requirements from a pip requirements file.

    Args:
        filename (str): The path to the requirements file.

    Returns:
        List[str]: A list of requirement strings.
    """
    with open(filename) as file:
        lineiter = (line.strip() for line in file)
        return [line for line in lineiter if line and not line.startswith("#")]

setup(
    name='pyspark_sales_data_project',
    version='0.1',
    packages=find_packages(),
    install_requires=parse_requirements('requirements.txt'),
    entry_points={
        'console_scripts': [
            'sales-data=src.main:main',
        ],
    },
)
