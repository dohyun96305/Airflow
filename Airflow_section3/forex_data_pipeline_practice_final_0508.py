# 08 => Send notification to Email and Slack

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator 
from airflow.providers.apache.spark.operators.spark_submit import SaprkSubmitOperator
from ariflow.operators.email import EmailOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

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

def _get_message() -> str : 
    return "This is Forex Data Pipeline"

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
            task_id = "Creating_forex_rates_table",                     # unique task_id in the same dag
            hive_cli_conn_id = "hive_conn",                             # id of the HiveOperator
                                                                        # hql want to execute, hql = HIVE version of SQL
            hql = """                                                                     
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

    Processing_forex_data = SaprkSubmitOperator(                        # SparkSubmitOperator : to interact with Spark  
        task_id = "Processing_forex_data",                              # unique task_id in the same dag
        application = "/opt/airflow/dags/scripts/forex_processing.py",  # path of the scripts want to excute with Spark ( ## this application not execute in Airflow, just trigger! )
        conn_id = "spark_submit_conn",                                  # id of the SparkSubmitOperator
        verbose = False                                                 # "Whether to pass the verbose flag to spark-submit process for debugging", choose whether to see complicated logs 
    )
    
    Send_email_notification = EmailOperator(                            # EmailOperator : to send email by using SMTP (Single Mail Transfer Protocol)
        task_id = "Send_email_notification",                            # unique task_id in the same dag
        to = "dohyun9653@gmail.com",                                    # certain email to send email
        subject = "forex_data-pipeline",                                # subject of email
                                                                        # content of email, can use jinja template
        html_content = 
                    """
                        Test_mail.<br/><br/> 
                        ninja template<br/>
                        {{ data_interval_start }}<br/>
                        {{ ds }}<br/>
                    """  
    )

    Send_slack_notification = SlackWebhookOperator(                     # SlackWebhookOperator : to send notification at Slack channel using Webhook
        task_id = "Send_slack_notification",                            # unique task_id in the same dag
        http_conn_id = "slack_conn",                                    # id of the SlackWebhookOperator, interact with Slack
        message = _get_message(),                                       # message to send, can use function
        channel = "airflow_alert"                                       # certain channel to send notification at Slack 
    )

    # To Define Dependencies between Task 
    # 1. set_downstream or set_upstream
    # Check_forex_rates_available.set_downstream(Check_forex_currencies_file_available)
    # Check_forex_currencies_file_available.set_upstream(Check_forex_rates_available)

    ### 2. >> (set_downstream), << (set_upstream)
    # Check_forex_rates_available >> Check_forex_currencies_file_available
    # Check_forex_currencies_file_available << Check_forex_rates_available

    # if want to wrtie dependencies with multiple line, not single line => Add last taske first in new line
    Check_forex_rates_available >> Check_forex_currencies_file_available >> Downloading_forex_rates >> Saving_forex_rates 
    Saving_forex_rates >> Creating_forex_rates_table >> Processing_forex_data
    Processing_forex_data >> Send_email_notification >> Send_slack_notification