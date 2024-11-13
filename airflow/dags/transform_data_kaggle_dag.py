from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from transformation_flight import transformation_flight  # Import from plugins

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    "transformation_flight_dag",
    default_args=default_args,
    description="A DAG to run the transformation_flight function",
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Define the PythonOperator task
    run_transformation_flight = PythonOperator(
        task_id="run_transformation_flight",
        python_callable=transformation_flight,
    )

    # Set task dependencies if needed
    run_transformation_flight