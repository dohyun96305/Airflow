from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook     # provide methods so can interact with an external service or tool
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.slack.notifications.slack_notifier import SlackNotifier

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata

from datetime import datetime

from include.S3_stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_csv, BUCKET_NAME

import requests

SYMBOL = "AAPL" # Specific company want to fetch stock prices

# Specify with Dag Decorater, Using Task Flow API
@dag(                                              
                                            # Argument 
    start_date = datetime(2024, 1, 1),      # Scheduled to starg dag
    schedule_interval = "@daily",           # Frequency to trigger dag
    catchup = False,                        # Not to run Non-Triggered Past DAG between start_date and current_date
    tags = ['stock_market'],                # To categorize to filter data pipelines on the Airflow UI

    on_success_callback = SlackNotifier(    # To send Message to Slack when DAG is succeed
        slack_conn_id = 'slack',            # Fetch the connection from the Airflow Meta DB (Admin - Connections)
        text = "The DAG stock_market has succeed", 
        channel = 'slack-airflow'           # Slack Channel to receive message
    ), 

    on_failure_callback = SlackNotifier(    # To send Message to Slack when DAG is failed
        slack_conn_id = 'slack',            # Fetch the connection from the Airflow Meta DB (Admin - Connections)
        text = "The DAG stock_market has failed", 
        channel = 'slack-airflow'           # Slack Channel to receive message
    )
)

# Create Python functions corresponding to DAG
def stock_market() : # Dag ID (Unique Identifier) of data pipeline

    # Specify Task with Decorateor @task
    # To check if API is available (Checking URL)
    @task.sensor(poke_interval = 30, timeout = 300, mode = 'poke') # Sensor Decorator : Wait for an event to happen before executing next task
    def is_api_available() -> PokeReturnValue : 
        api = BaseHook.get_connection('stock_api')                              # Fetch the connection from the Airflow Meta DB (Admin - Connections)
        url = f"{api.host}{api.extra_dejson['endpoint']}"                       # Specify URL to get response via api
        response = requests.get(url, headers = api.extra_dejson['headers'])     # Requests to URL 
        condition = response.json()['finance']['result'] is None                # Using response, Check whether the API is available 

        return PokeReturnValue(is_done = condition, xcom_value = url)           # Specify return value when the sensor is succeed, 
    
    # Specify Task with PythonOperator
    # To fetch the stock prices from the Finance API
    get_stock_prices = PythonOperator(
        task_id = 'get_stock_prices',                                           # Always need to define "Unique" task_id with PythonOperator
        python_callable = _get_stock_prices,                                    # To specify python function want to execute
        op_kwargs = {'url' : '{{ task_instance.xcom_pull(task_ids = "is_api_available") }}', 'symbol' : SYMBOL} # Dictionaries that Mapping Python fuction's parameters 
    )

    store_prices = PythonOperator(
        task_id = 'store_prices',
        python_callable = _store_prices,
        op_kwargs = {'stock' : '{{ task_instance.xcom_pull(task_ids = "get_stock_prices") }}'}  # Dictionaries that Mapping Python fuction's parameters 
    )

    format_prices = DockerOperator(
        task_id = 'format_prices',
        image = 'airflow/stock-app',                        # Docker Image to use
        container_name = 'format_prices',                   # Docker Container to use
        api_version = 'auto',                               # 
        auto_remove = True,                                 # Whether to delete the Docker Container once the task is finished
        docker_url = 'tcp://docker-proxy:2375',             # Allows the DockerOperator to use the current Docker environment
        network_mode = 'container:spark-master',            # Docker Container that DockerOperator should share the same network => Can communicate each other
        tty = True,                                         # Whether to interact with Docker Container
        xcom_all = False,                                   # Whether to want XCom
        mount_tmp_dir = False,                              # Whether to mount any temporary directory
        environment = {                                     # Parameter need to pass Argument to Spark Application App
            'SPARK_APPLICATION_ARGS' : '{{ task_instance.xcom_pull(task_ids = "store_prices") }}'
        }
    )

    get_formatted_csv = PythonOperator(
        task_id = 'get_formatted_csv', 
        python_callable = _get_formatted_csv, 
        op_kwargs = {
            'path' : '{{ task_instance.xcom_pull(task_ids = "store_prices") }}'
        }
    )

    # Astro SDK => For ETL 
    load_to_dw = aql.load_file(
        task_id = 'load_to_dw', 
        # Input File => File from Minio DB 
        input_file = File(path = f's3://{BUCKET_NAME}/{{{{ task_instance.xcom_pull(task_ids = "get_formatted_csv") }}}}', conn_id = 'minio'),
        
        # Output Table => File to load to Postgres (Open Source Database)
        output_table = Table(
            name = 'stock_market',  # Name of Table
            conn_id = 'postgres',   # Connect to Postgres
            metadata = Metadata(
                schema = 'public'
            )
        )
    )

    # Need to call Task functions when using task decorator (@task)
    # Define dependencies between different tasks from DAG
    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw

stock_market() # Need to call DAG functions 