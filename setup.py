from setuptools import setup, find_packages

def parse_requirements(filename):
    """
    Load requirements from a pip requirements file.
    """
    lineiter = (line.strip() for line in open(filename))
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