from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from helper.notify import notify_on_success, notify_on_error, notify_on_retry

default_args = {
    "owner": "dpd.kerja",
    "start_date": datetime(2025, 2, 2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_success_callback": notify_on_success,
    "on_failure_callback": notify_on_error,
    "on_retry_callback": notify_on_retry,
}

with DAG(
    "3_dbt", 
    default_args=default_args, 
    schedule_interval="0 3 * * *", 
    catchup=False,
    tags=["dbt"],
    max_active_runs=3,
    
    ) as dag:

    dbt_run_prep = BashOperator(
        task_id="dbt_preparation",
        bash_command="docker exec -i dbt_dpd dbt run --project-dir /usr/app/dbt --profiles-dir /usr/app/dbt --select preparation"
    )

    dbt_run_dimfact = BashOperator(
        task_id="dbt_dimfact",
        bash_command="docker exec -i dbt_dpd dbt run --project-dir /usr/app/dbt --profiles-dir /usr/app/dbt --select dimfact"
    )

    dbt_run_marts= BashOperator(
        task_id="dbt_mart",
        bash_command="docker exec -i dbt_dpd dbt run --project-dir /usr/app/dbt --profiles-dir /usr/app/dbt --select datamart"
    )

    dbt_run_prep >> dbt_run_dimfact >> dbt_run_marts
