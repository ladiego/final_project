import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from helper.helper_scrap import save_to_csv, load_to_bq
from helper.notify import notify_on_success, notify_on_error, notify_on_retry

# Konfigurasi default untuk DAG
default_args = {
    'owner': 'dpd.kerja',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 8),  
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_success_callback': notify_on_success,
    'on_failure_callback': notify_on_error,
    'on_retry_callback': notify_on_retry,
}

# Definisi DAG
dag = DAG(
    'scraping_webadapundi',
    default_args=default_args,
    schedule_interval='0 3 * * *',  # Menjalankan setiap hari jam 3 pagi
    catchup=False,  # Agar tidak menjalankan task di tanggal-tanggal sebelumnya
    tags=['scraping', 'bigquery'],
)

# Task untuk scraping dan menyimpan data ke CSV
save_task = PythonOperator(
    task_id='save_to_csv',
    python_callable=save_to_csv,
    dag=dag,
    execution_timeout=timedelta(minutes=15),
)

# Task untuk mengupload data ke BigQuery
load_task = PythonOperator(
    task_id='load_to_bq',
    python_callable=load_to_bq,
    dag=dag,
    execution_timeout=timedelta(minutes=15),
)

# Urutan eksekusi task
save_task >> load_task
