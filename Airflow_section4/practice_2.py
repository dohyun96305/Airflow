from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

# set dependencies at tasks
# 1. depends_on_past 
# 2. wait_for_downstream 

default_args = {
    'start_date': datetime(2019, 1, 1),
    'owner': 'dohyun', 
    'wait_for_downstream' : True   
}

def second_task():
    print('Hello from second_task')
    #raise ValueError('This will turns the python task in failed state')

def third_task():
    print('Hello from third_task')
    #raise ValueError('This will turns the python task in failed state')

with DAG(dag_id='depends_task', schedule_interval="0 0 * * *", default_args=default_args) as dag:
    
    bash_task_1 = BashOperator(task_id='bash_task_1', bash_command="echo 'first task'")
    python_task_2 = PythonOperator(task_id='python_task_2', python_callable=second_task, depends_on_past = True)
    python_task_3 = PythonOperator(task_id='python_task_3', python_callable=third_task)

    bash_task_1 >> python_task_2 >> python_task_3