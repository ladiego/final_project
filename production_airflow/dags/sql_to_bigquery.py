import os
import logging
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

BIGQUERY_KEY_PATH = os.getenv("BIGQUERY_KEY_PATH", "/opt/airflow/keys/gcp_keys.json")
BIGQUERY_PROJECT = "purwadika"
BIGQUERY_DATASET = "diego_library_final_project"
LOCAL_TZ = pytz.timezone("Asia/Jakarta")
TABLES = ["users", "books", "rents"]

# Updated table schema
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
            bigquery.SchemaField("publisher", "STRING"),
            bigquery.SchemaField("release_year", "INT64"),
            bigquery.SchemaField("stock", "INT64"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ],
        "columns": ["book_id", "title", "author", "publisher", "release_year", "stock", "created_at"]
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

def save_to_csv(**kwargs):
    """Save a DataFrame to CSV and return the file path."""
    dataframe = kwargs['ti'].xcom_pull(task_ids=kwargs['extract_task_id'])
    
    logging.info(f"Extracted DataFrame for {kwargs['table_name']}: {dataframe}")

    if dataframe is None or isinstance(dataframe, str):
        raise ValueError("Expected a DataFrame, but got a string or None.")
    
    file_path = f"/tmp/{kwargs['table_name']}.csv"
    try:
        dataframe.to_csv(file_path, index=False)
        logging.info(f"Saved {kwargs['table_name']} to {file_path}")
    except Exception as e:
        logging.error(f"Failed to save {kwargs['table_name']} to CSV: {e}")
        raise
    return file_path

def load_data_to_staging(file_path, table_name):
    """Loads a CSV file into the staging table."""
    logging.info(f"Loading data from {file_path} into staging table {table_name}_staging")
    
    # Check if the file exists
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")

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
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # Replace existing data
            schema=table_schema[table_name]["schema"],  # Use the same schema for staging
            max_bad_records=10,
            ignore_unknown_values=True,
        )
        with open(file_path, "rb") as file_data:
            load_job = client.load_table_from_file(file_data, table_id, job_config=job_config)
            load_job.result()  # Wait for the job to complete
            logging.info(f"Data loaded successfully into {table_id}")

        # Delete the CSV file after loading
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
    "on_success_callback": notify_on_success,
    "on_error_callback": notify_on_error,
    "on_retry_callback": notify_on_retry,
}

with DAG(
    "extract_load_to_bigquery",  
    start_date=datetime(2024, 12, 1),
    schedule_interval="0 1 * * *",
    catchup=False,
    tags=["ingestion", "postgresql", "bigquery"],
    default_args=default_args,
    max_active_runs=3,
) as dag:

    start = DummyOperator(task_id="start")

    # Create TaskGroups for each table
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
                    "table_name": table
                },
            )
            load_staging_task = PythonOperator(
                task_id=f"load_staging_{table}",
                python_callable=load_data_to_staging,
                op_kwargs={
                    "file_path": "{{ task_instance.xcom_pull(task_ids='process_" + table + ".save_" + table + "') }}",
                    "table_name": table
                },
            )
            load_final_task = PythonOperator(
                task_id=f"load_final_{table}",
                python_callable=load_data_to_final,
                op_kwargs={
                    "table_name": table
                },
            )

            extract_task >> save_task >> load_staging_task >> load_final_task

    end = DummyOperator(task_id="end")

    start >> [process_group for table in TABLES] >> end
