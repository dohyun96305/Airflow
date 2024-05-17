from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta

import pendulum

local_tz = pendulum.timezone("Europe/Paris")

default_args = {
    "start_date": datetime(2019, 1, 1, tzinfo = local_tz), 
    "owner": "dohyun"
}
# start_date
# DAGS configiured in UTC default
# tzinfo => specify the timezone of UTC to start_date
# tzinfo is defined => Aware Timezone, tzinfo is not defined => Naive Timezone

with DAG(dag_id = "practice_dag", 
         schedule_interval = "0 * * * *", 
         default_args = default_args,
         catchup = True) as dag:
    
# schedule_interval
# "0 * * * *" => schedule_interval, cron expressions, every 0 min = 1hours
# timedelta(hours = 1) => can configure schedule_interval as timedelta

# cron expressions => ignore UTC differences
# timedelta expressions => concern UTC differences

# catchup  
# default => True 
# also can change in airflow.cfg => "catchup_by_default"

# backfill CLI 
# airflow backfill -s "START_DATE" -e "END_DATE" --rerun_failed_tasks -B backfill
# start_date, end_date => Interval want to backfill
# --rerun_failed_tasks : auto rerun about all failed tasks
# -B backfill : force to run tasks starting from the recent days in first => process backward

    bash_task_1 = BashOperator(task_id='bash_task_1', bash_command="echo 'first task'")
    bash_task_2 = BashOperator(task_id='bash_task_2', bash_command="echo 'second task'")
        
    run_dates = dag.get_run_dates(start_date=dag.start_date)
    next_execution_date = run_dates[-1] if len(run_dates) != 0 else None
    
    # Logs to help you (printed from the web server logs)
    # comment when you use the DAG, Uncomment when not
    """
    print('datetime from Python is Naive: {0}'.format(timezone.is_naive(datetime(2019, 1, 1))))
    print('datetime from Airflow is Aware: {0}'.format(timezone.is_naive(timezone.datetime(2019, 1, 1)) == False))
    print('[DAG:practice_dag] timezone: {0} - start_date: {1} - schedule_interval: {2} - Last execution_date: {3} - next execution_date {4} in UTC - next execution_date {5} in local time'.format(
        dag.timezone, 
        dag.default_args['start_date'], 
        dag._schedule_interval, 
        dag.latest_execution_date, 
        next_execution_date,
        local_tz.convert(next_execution_date) if next_execution_date is not None else None
        ))
    """ 
    
    bash_task_1 >> bash_task_2
