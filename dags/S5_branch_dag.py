import airflow
import requests
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator

default_args = {
    'owner' : 'Airflow',
    'start_date' : airflow.utils.dates.days_ago(2),
}

IP_GEOLOCATION_APIS = {
    'ip-api' : 'http://ip-api.com/json/',
    'ipstack' : 'https://api.ipstack.com/',
    'ipinfo' : 'https://ipinfo.io/json'
}

# Try to get the country_code field from each API
# If given, the API is returned and the next task corresponding
# to this API will be executed
def check_api() :
    api_list = []

    for api, link in IP_GEOLOCATION_APIS.items() :
        r = requests.get(link)
        try :
            data = r.json()
            if data and 'country' in data and len(data['country']) : 
                api_list.append(api)
            
        except ValueError :
            pass

    return api_list if len(api_list) > 0 else 'none'

with DAG(dag_id = 'branch_dag', 
         default_args = default_args, 
         schedule_interval = "@once") as dag :

    # BranchPythonOperator
    # The next task depends on the return from the python function check_api
    check_api = BranchPythonOperator(
        task_id = 'check_api',
        python_callable = check_api
    )

    none = DummyOperator(
        task_id = 'none'
    )

    # By BranchPythonOperator, some tasks might be skipped, task will not be executed 
    # Because not all task has been executed and success, this task will not be executed and skipped
    # => Add trigger_rule parameter (Default value = 'all_success') 
    save = DummyOperator(task_id = 'save', trigger_rule = 'one_success') # At least one tasks has been succeed, this task will be executed

    check_api >> none >> save

    # Dynamically create tasks according to the APIs
    for api in IP_GEOLOCATION_APIS:
        process = DummyOperator(
            task_id=api
        )
    
        check_api >> process >> save