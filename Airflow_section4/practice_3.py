from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta

def on_success_dag(dict) :
    print("Success")
    print("dict")

def on_failure_dag(dict) : 
    print("Failure")
    print("dict")

def on_success_task(dict) : 
    print("Success")
    print("dict")

def on_failure_task(dict) : 
    print("Failure")
    print("dict")

default_args = {
    'start_date': datetime(2019, 1, 1),
    'owner': 'Airflow',

    'retries' : 3, 
    # number of retry, marking fail when retry more than retries
    'retry_delay' : timedelta(seconds=60), 
    # delay with a timedelta object between retries

    'emails' : ['email@email.com'], 
    'email_on_failure' : True, 
    'email_on_retry' : True,
    # need to configure SMTP server in airflow.cfg

    'on_success_callback' : on_success_task,
    'on_failure_callback' : on_failure_task, 
    
    'execution_timeout' : timedelta(seconds=60)
    # not recommended because each task can have different time to execute
    # can make each task
}

with DAG(dag_id = 'alert_dag', 
         schedule_interval = "0 0 * * *", 
         default_args = default_args, 
         catchup = True, 
         dagrun_timeout = timedelta(seconds=75), 
         on_success_callback = on_success_dag, 
         on_failure_callback = on_failure_dag) as dag: 
    
# To check callback log, check Airflow Scheduler logs
# get docker exec -it "DOCKER ID" /bin/bash to open Airlfow CLI, "cd logs/scheduler/" to check Each DAG logs  
    
    # Task
    t1 = BashOperator(task_id='t1', bash_command="exit 1")
    t2 = BashOperator(task_id='t2', bash_command="echo 'second task'")

    t1 >> t2