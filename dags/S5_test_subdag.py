from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.executors.sequential_executor import SequentialExecutor
from airflow.executors.celery_executor import CeleryExecutor

from include.S6_advanced_dag.subdag import factory_subdag

import airflow


DAG_NAME="test_subdag"

default_args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

with DAG(dag_id = DAG_NAME, 
         default_args = default_args, 
         schedule_interval = "@once") as dag:
    
    start = DummyOperator(
        task_id='start'
    )

    subdag_1 = SubDagOperator(
        task_id = 'subdag-1',
        subdag = factory_subdag(DAG_NAME, 'subdag-1', default_args), 
        # Parameter (Parent DAG name, subDAG name, Default parameter set for the parent DAG apply to subDAG as well)
        # Same default_args between parent DAG <-> subDAG => Need to keep consistency between the parent DAG and subDAGs to avoid unexpected behaviours

        # DEADLOCK : No tasks of any subdags can be excuted since all subdag instances have taken all the available slots
        # => Adding addtional queue, pools, workers for the subDAG to execute and not take the place of the subDAG tasks
        
        # executor (Default Setting : SequentialExecutor)
        # To run multiple tasks in parallel in subDAGs, using CeleryExecutor => Need to set parameter on Airflow.cfg - [core] - executor
    )

    some_other_task = DummyOperator(
        task_id = 'check'
        )

    subdag_2 = SubDagOperator(
        task_id = 'subdag-2',
        subdag = factory_subdag(DAG_NAME, 'subdag-2', default_args),
    )

    end = DummyOperator(
        task_id = 'final'
    )

    start >> subdag_1 >> some_other_task >> subdag_2 >> end