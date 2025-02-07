from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from helper.helper_postgres import generate_data, insert_data
from helper.notify import notify_on_success, notify_on_error, notify_on_retry

def generate_data_task(**kwargs):
    users, books = generate_data()
    # saving data to XCom
    kwargs['ti'].xcom_push(key='users', value=users)
    kwargs['ti'].xcom_push(key='books', value=books)

def insert_data_task(**kwargs):
    # retrieving data from XCom
    ti = kwargs['ti']
    users = ti.xcom_pull(key='users', task_ids='generate_data')
    books = ti.xcom_pull(key='books', task_ids='generate_data')
    insert_data(users, books)

default_args = {
    'owner': 'dpd.kerja',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),  
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    "on_success_callback": notify_on_success,
    "on_error_callback": notify_on_error,
    "on_retry_callback": notify_on_retry,
}

with DAG(
    dag_id='ingest_data_db',
    default_args=default_args,
    description='Insert data to PostgreSQL',
    schedule_interval='@hourly',  
    catchup=False,
    tags=['postgres', 'data-insertion'],
) as dag:
    generate_data_operator = PythonOperator(
        task_id='generate_data',
        python_callable=generate_data_task,
        provide_context=True,  # Provides context for XCom
    )

    insert_data_operator = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data_task,
        provide_context=True,  # Provides context for XCom
    )

    generate_data_operator >> insert_data_operator
