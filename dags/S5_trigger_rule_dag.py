import airflow
import requests
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator

default_args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(1),
}

def download_website_a():
    print("download_website_a")
    #raise ValueError("error")

def download_website_b():
    print("download_website_b")
    #raise ValueError("error")

def download_failed():
    print("download_failed")
    #raise ValueError("error")

def download_succeed(): 
    print("download_succeed")
    #raise ValueError("error")

def process():
    print("process")
    #raise ValueError("error")

def notif_a():
    print("notif_a")
    #raise ValueError("error")

def notif_b():
    print("notif_b")
    #raise ValueError("error")

with DAG(dag_id='trigger_rule_dag', 
    default_args=default_args, 
    schedule_interval="@daily") as dag:

    # all_success => When all upstream tasks is succeed
    # all_failed => When all upstream tasks is failed
    # all_done => When all upstream task is executed whatever its state

    # one_failed => When one of upstream task is failed, without waiting other task to finsih
    # one_success => When one of upstream task is succeeded, without waiting other task to finish 
    
    # none_failed => When all of upstream task is not failed,
    # none_skipped => when all of upstream task is not skipped,
    
    download_website_a_task = PythonOperator(
        task_id='download_website_a',
        python_callable=download_website_a
    )

    download_website_b_task = PythonOperator(
        task_id='download_website_b',
        python_callable=download_website_b
    )

    download_failed_task = PythonOperator(
        task_id='download_failed',
        python_callable=download_failed,
        trigger_rule="all_failed" ###
    )

    download_succeed_task = PythonOperator(
        task_id='download_succeed',
        python_callable=download_succeed,
        trigger_rule="all_success"
    )

    process_task = PythonOperator(
        task_id='process',
        python_callable=process,
        trigger_rule="one_success" ###
    )

    notif_a_task = PythonOperator(
        task_id='notif_a',
        python_callable=notif_a,
        trigger_rule="none_failed" ###
    )

    notif_b_task = PythonOperator(
        task_id='notif_b',
        python_callable=notif_b,
        trigger_rule="one_failed" ###
    )

    # Implement dependencies below
    # ie:
    # a >> b        : b depends on a
    # [a b] >> c    : c depends on a and b
    # ...

    
    [download_website_a_task, download_website_b_task] >> download_succeed_task 
    [download_website_a_task, download_website_b_task] >> download_failed_task

    [download_succeed_task, download_failed_task] >> process_task
    process_task >> [notif_a_task, notif_b_task]
    