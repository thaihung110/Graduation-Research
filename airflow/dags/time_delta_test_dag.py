from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries': 1,
}

dag = DAG(
    'time_delta_test_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=None,
)

def print1():
    print('log-1')

def print2():
    print('log-2')

def delay():
    time.sleep(10)

task_1 = PythonOperator(
    task_id='task_1',
    python_callable=print1,
    dag=dag,
)

delay_task = PythonOperator(
    task_id='delay_task',
    python_callable=delay,
)

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=print2,
    dag=dag,
)

#task flow
task_1 >> delay_task >> task_2