from airflow.decorators import dag, task

from datetime import datetime, timedelta

from include.S4_airflow_feature.functions1 import second_task, third_task, on_success_dag, on_failure_dag, on_success_task, on_failure_task

# depends_on_past 
### Check past DAG run about same Task instance
### If same Task in past DAG run is failed, Not even queued in current DAG

# wait_for_downstreams 
### Wait for downstream of all the past instance Tasks of past DAGS
### If past DAG are failed, then all tasks in current DAG will not queued

# To check callback log, check Airflow Scheduler logs
### get docker exec -it "DOCKER Airflow ID" /bin/bash to open Airlfow CLI, "cd logs/scheduler/" to check Each DAG logs  
    

default_args = {
    'owner': 'dohyun', 
    'wait_for_downstreams' : 'True', 

    'retries' : 3, # Number of retry, marking Failed when retry more than retries
    'retry_delay' : timedelta(seconds=60), # Delay with a timedelta object between retries

    'emails' : ['email@email.com'], 
    'email_on_failure' : True, 
    'email_on_retry' : True,
    # need to configure SMTP server in airflow.cfg

    'on_success_callback' : on_success_task,
    'on_failure_callback' : on_failure_task, 
}


@dag(   
    default_args = default_args,

    start_date = datetime(2024, 1, 1),          # Define start_date 
    schedule = '0 3 * * *',                     # Define Interval of Exectution of DAG Runs, Using Cron Expression, Need to concern UTC 
    max_active_runs = 1,                        # Define number of DAG runs at a time for a specific DAG
    catchup = False,                            # Whether to runs all the non-triggered Past DAGS between (the latest DAG run) and (current_date)
    
    dagrun_timeout = timedelta(seconds=75),     # Set limit time to running DAG, If more time need to run, DAGRun will be failed, but next DAGRun can be able to run
    on_success_callback = on_success_dag, 
    on_failure_callback = on_failure_dag,
    
    tags = ['practice'],
)

def practice() : 
    @task()
    def task_1() : 
        print('@@@@@@@@@@@@@@@@@@@@@@@ Airflow Feature Practice @@@@@@@@@@@@@@@@@@@@@@@')

    @task()
    def task_2(depends_on_past = True) : 
        second_task()

    @task
    def task_3() : 
        third_task()

    task_1() >> task_2() >> task_3()

practice()
