from datetime import datetime, timedelta

from druid_ingestion import (  # Import the ingestion functions
    submit_dim_airline_ingestion_spec,
    submit_dim_airport_ingestion_spec,
    submit_dim_date_ingestion_spec,
    submit_fact_flight_ingestion_spec,
)

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
    description="A DAG to run the transformation_flight function and load data into Druid",
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Define the PythonOperator task for data transformation
    run_transformation_flight = PythonOperator(
        task_id="run_transformation_flight",
        python_callable=transformation_flight,
    )

    # Define the Druid ingestion tasks
    dim_date_task = PythonOperator(
        task_id="dim_date_ingestion_task",
        python_callable=submit_dim_date_ingestion_spec,
    )

    dim_airport_task = PythonOperator(
        task_id="dim_airport_ingestion_task",
        python_callable=submit_dim_airport_ingestion_spec,
    )

    dim_airline_task = PythonOperator(
        task_id="dim_airline_ingestion_task",
        python_callable=submit_dim_airline_ingestion_spec,
    )

    fact_flight_task = PythonOperator(
        task_id="fact_flight_ingestion_task",
        python_callable=submit_fact_flight_ingestion_spec,
    )

    # Set task dependencies
    run_transformation_flight >> [
        dim_date_task,
        dim_airport_task,
        dim_airline_task,
        fact_flight_task,
    ]
