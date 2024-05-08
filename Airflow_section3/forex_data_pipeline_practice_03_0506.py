# 03 => Add FileSensor

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor

from datetime import datetime, timedelta

default_args = {                                    # common arguments applied to Task, not dags
    'owner' : 'dohyun_yoon',
    'email_on_failure' : False, 
    'email_on_retry' : False, 
    'email' : 'dohyun96305@naver.com', 
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 5)
}

with DAG('forex_data_pipeline',                     # unique dag id
         start_date = datetime(2024, 1, 1),         # scheduled to starg dag
         schedule_interval = "@daily",              # frequency to trigger dag
         default_args = default_args,               # common argument to apply task that in dag
         catchup = False                            # Not to run DAG between start_date and current_date
         ) as dag : 
    
    Check_forex_rates_available  = HttpSensor(                          # HttpSensor : to check API (URL) with certain condition
        task_id = 'Check_forex_rates_available',                        # unique task_id in the same dag
        http_conn_id = 'forex_api',                                     # id of the HttpSensor
        endpoint = "marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",     # after the host where url have, have to consider with Airflow UI CONN (HOST)
        response_check = lambda response : "rates" in response.text,    # specify python function to check whether is expected
        poke_interval = 5,                                              # the frequency that sensor is going to check condition
        timeout = 20                                                    ##### the time to task end up in failure, not to run sensor forever
    )

    Check_forex_currencies_file_available = FileSensor(                 # FileSensor : to check file or folder path 
        task_id = "Check_forex_currencies_file_available",              # unique task_id in the same dag
        fs_conn_id = "forex_path",                                      # id of the FileSensor 
        filepath = "forex_currencies.csv",                              # name of the file looking in File or Folder path, have to consider with Airflow UI CONN (EXTRA - PATH)
        poke_interval = 5,                                              # the frequency that sensor is going to check condition
        timeout = 20                                                    ##### the time to task end up in failure, not to run sensor forever
    )

    # Go to Airflow UI to connect FileSensor

    # Admin - Connections - Add New Connection to add FileSensor Conn
    # Conn id => fs_conn_id
    # Conn type => FileSensor : file (path)
    ### Extra => {"path" : "FILE OR FOLDER PATH TO CHECK", file}, 

    ##### To test task
    # 1. docker exec -it "DOCKER AIRFLOW CONTAINER ID" /bin/bash
    # 2. airflow tasks test "DAG ID" "TASK_ID" " EXECUTION DATE"
    # 3. check success or failure

    # can enter to docker-airflow CLI => always test task before run DAG