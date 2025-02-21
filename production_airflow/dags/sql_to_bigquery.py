import os
import logging
import pandas as pd
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from helper.ext_sql import extract_table_data
from helper.crt_dataset_table import create_dataset_if_not_exists, create_staging_table, create_final_table_if_not_exists
from helper.notify import notify_on_success, notify_on_error, notify_on_retry

# logging config
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BIGQUERY_KEY_PATH = os.getenv("BIGQUERY_KEY_PATH","/opt/airflow/keys/gcp_keys.json")
BIGQUERY_PROJECT = "purwadika"
BIGQUERY_DATASET = "diego_library_finpro"
LOCAL_TZ = pytz.timezone("Asia/Jakarta")
TABLES = ["users", "books", "rents"]

table_schema = {
    "users": {
        "schema": [
            bigquery.SchemaField("user_id", "INT64"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("address", "STRING"),
            bigquery.SchemaField("gender", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ],
        "columns": ["user_id", "name", "email", "address", "gender", "created_at"]
    },
    "books": {
        "schema": [
            bigquery.SchemaField("book_id", "INT64"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("author", "STRING"),
            bigquery.SchemaField("book_type", "STRING"),
            bigquery.SchemaField("genre", "STRING"),
            bigquery.SchemaField("publisher", "STRING"),
            bigquery.SchemaField("release_year", "INT64"),
            bigquery.SchemaField("stock", "INT64"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ],
        "columns": ["book_id", "title", "author", "book_type", "genre", "publisher", "release_year", "stock", "created_at"]
    },
    "rents": {
        "schema": [
            bigquery.SchemaField("rent_id", "INT64"),
            bigquery.SchemaField("user_id", "INT64"),
            bigquery.SchemaField("book_id", "INT64"),
            bigquery.SchemaField("rent_date", "TIMESTAMP"),
            bigquery.SchemaField("return_date", "TIMESTAMP"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ],
        "columns": ["rent_id", "user_id", "book_id", "rent_date", "return_date", "created_at"]
    },
}

def convert_date(dataframe, timestamp_columns):
    """
    Convert Unix epoch timestamps (in milliseconds) to human-readable format (YYYY-MM-DD HH:MM:SS).

    Args:
        dataframe (pd.DataFrame): DataFrame containing the timestamp columns.
        timestamp_columns (list): List of column names to convert.

    Returns:
        pd.DataFrame: DataFrame with converted timestamp columns.
    """
    for col in timestamp_columns:
        if col in dataframe.columns:
            # Convert Unix epoch (milliseconds) to datetime
            dataframe[col] = pd.to_datetime(dataframe[col], unit='ms').dt.strftime('%Y-%m-%d %H:%M:%S')
    return dataframe

def save_to_csv(**kwargs):
    """Save a DataFrame to CSV and return the file path."""
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids=kwargs['extract_task_id'])

    logging.info(f"Extracted data from XCom for {kwargs['table_name']}: {data}")

    if data is None or data == "null":
        logging.error("Extracted data is None or empty JSON.")
        raise ValueError("Expected a DataFrame, but got None.")

    try:
        # Try JSON parsing to ensure data is valid
        if isinstance(data, str):
            # If data is a JSON string, parse it
            json.loads(data)  # Validate JSON
            dataframe = pd.read_json(data)
        else:
            # Assume data is already a DataFrame
            dataframe = data

        # Convert Unix epoch timestamps to human-readable format
        timestamp_columns = ["rent_date", "return_date", "created_at"]
        dataframe = convert_date(dataframe, timestamp_columns)

        file_path = f"/opt/airflow/shared/{kwargs['table_name']}.csv"
        dataframe.to_csv(file_path, index=False)
        logging.info(f"Saved {kwargs['table_name']} to {file_path}")
        logging.info(f"CSV file contents:\n{dataframe.head()}")
        return file_path
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON data: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to save {kwargs['table_name']} to CSV: {e}")
        raise

def load_data_to_staging(**kwargs):
    """Loads a CSV file into the staging table."""
    ti = kwargs['ti']
    table_name = kwargs['table_name']
    
    # Retrieve file path from XCom
    save_task_id = f"process_{kwargs['table_name']}.save_{kwargs['table_name']}"
    file_path = ti.xcom_pull(task_ids=save_task_id)

    logging.info(f"Retrieved file path from XCom for {table_name}: {file_path}")

    if not file_path or not isinstance(file_path, str):
        logging.error(f"Invalid file path in XCom for task {save_task_id}.")
        raise ValueError(f"Invalid file path in XCom for task {save_task_id}.")

    # Make sure the file exists before loading
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(file_path, "r") as file:
        lines = file.readlines()
        logging.info(f"First 5 lines of {file_path}:\n{''.join(lines[:5])}")

    try:
        credentials = service_account.Credentials.from_service_account_file(
            BIGQUERY_KEY_PATH, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        # Ensure the dataset exists
        create_dataset_if_not_exists(client, BIGQUERY_PROJECT, BIGQUERY_DATASET)

        # Load data into the staging table (replace existing data)
        staging_table_name = create_staging_table(client, BIGQUERY_PROJECT, BIGQUERY_DATASET, table_name, table_schema[table_name]["schema"])
        table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{staging_table_name}"

        # Ensure the schema is sent to BigQuery
        schema = table_schema[kwargs['table_name']]["schema"]

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=schema,  
            max_bad_records=100,
            ignore_unknown_values=True,
            source_format=bigquery.SourceFormat.CSV,  
            skip_leading_rows=1,  
        )

        with open(file_path, "rb") as file_data:
            load_job = client.load_table_from_file(file_data, table_id, job_config=job_config)
            load_job.result()  # Wait for the job to finish

        logging.info(f"Data loaded successfully into {table_id}")

        # Delete files after success
        os.remove(file_path)
        logging.info(f"Deleted file: {file_path}")

    except Exception as e:
        logging.error(f"Failed to load data into staging table: {e}")
        raise

def load_data_to_final(table_name):
    """Loads unique data from the staging table into the final table."""
    logging.info(f"Loading unique data from staging table to final table {table_name}")
    
    try:
        credentials = service_account.Credentials.from_service_account_file(
            BIGQUERY_KEY_PATH, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        # Create the final table if it does not exist
        create_final_table_if_not_exists(client, BIGQUERY_PROJECT, BIGQUERY_DATASET, table_name, table_schema[table_name]["schema"])

        # Define the final and staging table IDs
        final_table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{table_name}"
        staging_table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{table_name}_staging"

        # Define the merge query based on the table name
        merge_query = f"""
            MERGE `{final_table_id}` AS final
            USING (
                SELECT DISTINCT * FROM `{staging_table_id}`
                WHERE created_at IS NOT NULL
            ) AS staging
            ON final.{table_schema[table_name]['columns'][0]} = staging.{table_schema[table_name]['columns'][0]}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(table_schema[table_name]['columns'])})
                VALUES ({', '.join([f'staging.{col}' for col in table_schema[table_name]['columns']])})
        """
        
        # Execute the merge query
        query_job = client.query(merge_query)
        query_job.result()  # Wait for the job to complete
        logging.info(f"Unique data merged from staging table to final table {final_table_id}")

    except Exception as e:
        logging.error(f"Failed to load data into final table: {e}")
        raise

default_args = {
    "owner": "dpd.kerja",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ['dpd.kerja@gmail.com'],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_on_error,
    "on_retry_callback": notify_on_retry,
}

with DAG(
    "2_extract_load_to_bigquery",  
    start_date=datetime(2024, 12, 1),
    schedule_interval="0 1 * * *",
    catchup=False,
    tags=["ingestion", "postgresql", "bigquery"],
    default_args=default_args,
    max_active_runs=3,
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    task_groups = []

    for table in TABLES:
        with TaskGroup(group_id=f"process_{table}") as process_group:
            
            extract_task = PythonOperator(
                task_id=f"extract_{table}",
                python_callable=extract_table_data,
                op_kwargs={"table": table},
            )

            save_task = PythonOperator(
                task_id=f"save_{table}",
                python_callable=save_to_csv,
                op_kwargs={
                    "extract_task_id": f"process_{table}.extract_{table}",
                    "table_name": table,
                },
            )

            load_staging_task = PythonOperator(
                task_id=f"load_staging_{table}",
                python_callable=load_data_to_staging,
                op_kwargs={
                    "table_name": table,
                },
            )

            load_final_task = PythonOperator(
                task_id=f"load_final_{table}",
                python_callable=load_data_to_final,
                op_kwargs={"table_name": table},
            )

            # Define dependencies within TaskGroup
            extract_task >> save_task >> load_staging_task >> load_final_task

        # Save the TaskGroup to a list so it can be associated with start
        task_groups.append(process_group)

    start >> task_groups >> end
