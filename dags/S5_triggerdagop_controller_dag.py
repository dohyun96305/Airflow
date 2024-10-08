import pprint as pp
import airflow.utils.dates
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
        "owner" : "airflow", 
        "start_date" : airflow.utils.dates.days_ago(1)
    }

def conditionally_trigger(context, dag_run_obj) :
    # dag_run_obj : Automatically given by the TriggerDagRunOperator 
    #               Simple class composed by a run_id, payload

    if context['params']['condition_param'] :
        dag_run_obj.payload = {                             # Allows to send data from the Controller DAG to Target DAG
                'message' : context['params']['message']
            }

        pp.pprint(dag_run_obj.payload)
        return dag_run_obj

with DAG(dag_id = "triggerdagop_controller_dag", 
         default_args = default_args, 
         schedule_interval = "@once") as dag :

    trigger = TriggerDagRunOperator(
        task_id = "trigger_dag",
        trigger_dag_id = "triggerdagop_target_dag",         # Dag_ID want to control
        provide_context = True,                             # Default => True, Set True to provide parameters to python_callable function                    
        python_callable = conditionally_trigger,        
        params = {
            'condition_param' : True, 
            'message' : 'Hi from the controller'
        },
    )

    last_task = DummyOperator(task_id = "last_task")

    trigger >> last_task