import pprint as pp
import airflow.utils.dates
from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta


# For using ExternalTaskSensor, recommend to set both DAG same start_date and schedule_dag 
default_args = {
        "owner" : "airflow", 
        "start_date" : airflow.utils.dates.days_ago(1)
    }

with DAG(dag_id = "externaltasksensor_dag", 
         default_args = default_args, 
         schedule_interval = "@daily") as dag :

    sensor = ExternalTaskSensor(
        task_id = 'sensor',
        external_dag_id = 'sleep_dag',      # External DAG_id waiting
        external_task_id = 't2'             # External Task_id waiting
        
        # Waiting for < Task 't2' in DAG 'sleep_dag' >

    last_task = DummyOperator(task_id = "last_task") # Task will be excuted when ExternalTaskSensor is excuted 

    sensor >> last_task