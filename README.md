# add Upsert Functionality to DataLakehouses with PySpark and Iceberg

## Overview

This project demonstrates how to perform upsert operations using PySpark and Iceberg with PostgreSQL as the backend database. Upsert, also known as merge, is a database operation that allows you to insert new records and update existing records within a single operation.

## Project Structure

- `notebooks/`: Contains Jupyter notebooks demonstrating the upsert functionality.
  - `upserts_with_pyspark.ipynb`: Main notebook showcasing the upsert operations.
  - `IcebergDemo.ipynb`: Demonstrates the use of Iceberg with Spark.
  - `Upserts with Iceberg.ipynb`: Another example of upserts using Iceberg with only spark-sql.
  - `utils.py`: Utility functions used in the notebooks.
  - `config.py`: Configuration file for database credentials and other settings.
  - `consts.py`: Constants used in the notebooks.
- `warehouse/`: Database Directory at MINIO that contains the iceberg tables.
- `Dockerfile`: Dockerfile to set up the environment.
- `Docker-compose.yaml`: Docker Compose file to orchestrate the services.

## Upsert Functionality

The upsert functionality is implemented in the `notebooks/upserts_with_pyspark.ipynb` notebook. The main steps involved are:

1. **Read Data**: Load data from CSV files into Spark DataFrames.
2. **Detect Schema Changes**: Identify new or dropped columns in the source table and apply these schema changes to the target table.
3. **Detect Record Changes**: Identify new, updated, and deleted records by comparing source and target DataFrames using hashing and a full outer join.
4. **Apply Changes to Data Lake**: Merge the changes into the target table in the data lake.
5. **Apply Changes to Database**: After merging changes to the data lake, connect to the PostgreSQL database and apply the same changes to the target table in the database.

![upserts_pipeline](https://github.com/user-attachments/assets/e75f6bb5-2a61-467a-8c8d-e35e438b1514)


### Key Functions

- `alter_datalake_target`: This function alters the target table schema to match the source table schema if there are any new or dropped columns.
- `upsert_datalake_target`: This function performs the upsert operation by merging changes from the source table into the target table in the data lake.
- `alter_database_target`: This function alters the target table schema in the PostgreSQL database to match the source table schema if there are any new or dropped columns.
- `upsert_database_target`: This function performs the upsert operation by merging changes from the source table into the target table in the PostgreSQL database.

![upserts_flowchart](https://github.com/user-attachments/assets/4aaa7e05-fea9-421a-af37-a4e9e41bfeca)


### Example Usage

```python
from utils import upsert_datalake_target, alter_datalake_target, upsert_database_target, alter_database_target
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("pyspark_upsert") \
    .getOrCreate()

# Define schema and table names
schema_name = "iceberg"
source_table = "source_customers"
target_table = "target_customers"
key_columns = ["CustomerID"]
upsert_flag = "cdc_flag"

# Detect and apply schema changes in data lake
alter_datalake_target(spark, schema_name, source_table, target_table)

# Detect and apply schema changes in database
alter_database_target(spark, schema_name, source_table, target_table)

# Detect record changes
changes = get_source_changes(spark, schema_name, source_table, target_table, key_columns)

# Perform upsert in data lake
upsert_datalake_target(spark, schema_name, source_table, target_table, key_columns, changes, upsert_flag)

# Perform upsert in database
upsert_database_target(spark, schema_name, source_table, target_table, key_columns, changes, upsert_flag)
