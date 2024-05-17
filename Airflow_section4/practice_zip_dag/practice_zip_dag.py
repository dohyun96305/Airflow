from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from functions.helpers import first_task, second_task, third_task

from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2019, 1, 1),
    'owner': 'Airflow'
}

with DAG(dag_id='packaged_dag', schedule_interval="0 0 * * *", default_args=default_args) as dag:

    # Task from helpers 
    python_task_1 = PythonOperator(task_id = 'python_task_1', python_callable = first_task)
    python_task_2 = PythonOperator(task_id = 'python_task_2', python_callable = second_task)
    python_task_3 = PythonOperator(task_id = 'python_task_3', python_callable = third_task)

    python_task_1 >> python_task_2 >> python_task_3


# to zip this dags and functions folder about task
# "zip -rm practice_zip_dag.zip practice_zip_dag.py functions/"
# to compress more than 1 file => listing all files 

# -rm => -r + -m
# -r : compress all file and all contents including subdirectories
# -m : compress the files and delete the original file 