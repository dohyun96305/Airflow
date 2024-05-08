# 06 => Add HiveOperator => Create Hive Table to interact with SQL

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator 

from datetime import datetime, timedelta

import csv
import requests
import json

# Download forex rates according to the currencies we want to watch
# described in the file forex_currencies.csv
def download_rates():
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }

    with open('/opt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')

        for idx, row in enumerate(reader):
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}

            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]

            with open('/opt/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')


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

    Downloading_forex_rates = PythonOperator(                           # PythonOperator : to execute certain python fucntion
        task_id = "Downloading_forex_rates",                            # unique task_id in the same dag
        python_callable = download_rates                                # certain python function want to execute
                                                                        # if python function need args or kwargs => op_kwargs (Dict) or op_args (List) to list argument
    )

    Saving_forex_rates = BashOperator(                                  # BashOperator : to excute certain bash command
        task_id = "Saving_forex_rates",                                 # unique task_id in the same dag
                                                                        # certain bash command want to execute => multi-line available (by using """ """)
        bash_command =  """                                              
            hdfs dfs -mkdir -p /forex && \                                          # make root path in HDFS 
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex       # copy Local Json file to HDFS 

        """
    )

    Creating_forex_rates_table = HiveOperator(                          # HiveOperator : to interact with HIVE 
            task_id="creating_forex_rates_table",                       # unique task_id in the same dag
            hive_cli_conn_id="hive_conn",                               # id of the HiveOperator
                                                                        # hql want to execute, hql = HIVE version of SQL
            hql="""                                                                     
                CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(        # Create Hive Table 
                    base STRING,
                    last_update DATE,
                    eur DOUBLE,
                    usd DOUBLE,
                    nzd DOUBLE,
                    gbp DOUBLE,
                    jpy DOUBLE,
                    cad DOUBLE
                    )
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                STORED AS TEXTFILE
            """
        )

    # Go to Airflow UI to connect HiveOperator

    # Admin - Connections - Add New Connection to add HiveOperator Conn
    # Conn id => hive_cli_conn_id
    # Conn type => Hive Server 2 Thrift
    # host => name of the hive host
    # Login, Password, Port 
    ### Extra => {"path" : "FILE OR FOLDER PATH TO CHECK", file}, 

    ##### To test task
    # 1. docker exec -it "DOCKER AIRFLOW CONTAINER ID" /bin/bash
    # 2. airflow tasks test "DAG ID" "TASK_ID" " EXECUTION DATE"
    # 3. check success or failure

    # can enter to docker-airflow CLI => always test task before run DAG