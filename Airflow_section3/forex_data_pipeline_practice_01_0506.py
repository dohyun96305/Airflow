# 01 => Initiate DAG

from airflow import DAG

from datetime import datetime, timedelta

default_args = { # common arguments applied to Task, not dags
    'owner' : 'dohyun_yoon',
    'email_on_failure' : False, 
    'email_on_retry' : False, 
    'email' : 'dohyun96305@naver.com', 
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 5)
}

with DAG('forex_data_pipeline',                 # unique dag id
         start_date = datetime(2024, 1, 1),     # scheduled to starg dag
         schedule_interval = "@daily",          # frequency to trigger dag
         default_args = default_args,           # common argument to apply task that in DAG
         catchup = False                        # Not to run DAG between start_date and current_date
         ) as dag : 
    
    None