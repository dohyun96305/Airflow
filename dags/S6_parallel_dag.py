from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime

# When Airflow is set to use the LocalExecutor, Needs to have a Relational Database running to work

### Can modify parameter parallelism directly in airflow.cfg 
# parallelism             : how many task instances for all DAGRuns can be actively running in parallel
# dag_concurrency         : how many task instances the scheduler is able to schedule at once per specific DAG
# max_active_runs_per_dag : Limits the maximum number of active DAGRuns per specific DAG

### Airflow UI - Data Profiling - Ad Hoc Query 
# Allows to query the DataBases want by selecting a connection
# Can disable this features to change parameter "secure_mode" to True

# - SELECT * FROM dag_run ;
# - SELECT * FROM task_instance ;
# ~~~ 

### Airflow UI - Admin - Connections
# To query the DatBases, Need to set connections to DataBases first 

default_args = {
    'start_date' : datetime(2019, 1, 1),
    'owner' : 'Airflow',
    'email' : 'owner@test.com'
}

def process(p1) :
    print(p1)

    return 'done'

with DAG(dag_id = 'parallel_dag', 
         schedule_interval = '0 0 * * *', 
         default_args = default_args, 
         catchup = False) as dag :
    
    # Tasks dynamically generated 
    tasks = [BashOperator(task_id = 'task_{0}'.format(t), bash_command = 'sleep 5'.format(t)) for t in range(1, 4)]
    # generate task_1, task_2, task_3 with BashOperator, bash_command = 'sleep 5'

    task_4 = PythonOperator(task_id = 'task_4', python_callable = process, op_args = ['my super parameter'])

    task_5 = BashOperator(task_id = 'task_5', bash_command = 'echo "pipeline done"')

    tasks >> task_4 >> task_5
    # By setting Dependencies between Tasks and parameter in airflow.cfg 
    # Get different execution time of each Tasks
        