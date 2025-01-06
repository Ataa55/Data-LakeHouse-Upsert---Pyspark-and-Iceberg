# DataLakeHouses Upserts with PySpark and Iceberg

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
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from sqlalchemy import create_engine, exc, text, inspect
import config
from utils import * 
import consts 


if __name__ == "__main__":

    #Glopal variables
    postgres_cred = config.POSTGRS_CREDENTIALS
    # spark variables
    spark_master = "spark://af27ca1c49e9:7077"
    app_name = "pyspark_upsert"
    memory = "2g"
    
    # data lake variables
    csv_file = "/home/iceberg/warehouse/Customers.csv"
    schema_name = "iceberg"
    db_schema_name = "public"
    source_table = "patition_source_customers"
    target_table = "patition_target_customers"
    key_column = ["CustomerID"]
    upsert_flag = "cdc_flag"
    db_table = "costumers"
    changes_table = f"{db_table}_cdc"
    
    # start spark session 
    spark = start_spark_session(spark_master = spark_master, 
                                app_name = app_name, 
                                memory = memory)

    
    
    df = spark.read.csv(csv_file, 
                        header=True, 
                        inferSchema=True)
    
    df_limited = df.limit(15)

    #create data bsae at iceberg if not exists
    spark.sql(f"create database if not exists {schema_name}")

    # write the data as iceberg tables
    df_limited.write.format("iceberg").saveAsTable(f"{schema_name}.{source_table}",
                                                   mode="overwrite")
    df_limited.write.format("iceberg").saveAsTable(f"{schema_name}.{target_table}")


    # get the source and target iceberg tables
    df_source = spark.sql(f"select * from {schema_name}.{source_table}")
    df_target = spark.sql(f"select * from {schema_name}.{target_table}")

    # alter any scchema changes at the target datalake table and get the source schema changes
    source_new_actions = alter_datalake_target(spark, 
                                               schema_name, 
                                               source_table, 
                                               target_table)

    # get the new record actions (inserts, updates, deletes)
    changes, join_result = get_source_changes(spark, 
                                              schema_name, 
                                              source_table, 
                                              target_table, 
                                              key_column)
    
    # write the changes at the database
    write_sdf_to_postgres_db(changes, 
                             config.POSTGRS_CREDENTIALS, 
                             changes_table, 
                             mode = "overwrite")
    
    # upsert those changes at the datalake
    upsert_datalake_target(spark, 
                           schema_name, 
                           source_table, 
                           target_table,
                           key_column,
                           changes, 
                           upsert_flag)

    # connect to the database
    engine = connect_to_db(postgres_cred)

    # alter any scchema changes at the target datalake table and get the source schema changes
    alter_db_target(engine, db_table, db_schema_name, source_new_actions)

    # upsert (inserts, deletes, upates) changes at the database target
    upsert_db_target(engine, db_table, db_schema_name)
