# Script to add new DAGs folders using the class DagBag
# Paths must be absolute
# Only can see error by checking Docker Web Server log, not Airflow UI

'''
import os
from airflow.models import DagBag

dags_dirs = [
                '/usr/local/airflow/project_a', 
                '/usr/local/airflow/project_b'
            ]

# create a dagbag for each path given from the array "dags_dirs"
# Airflow must be able to access given paths at "dags_dirs"

for dir in dags_dirs:
   dag_bag = DagBag(os.path.expanduser(dir))

   if dag_bag:
      for dag_id, dag in dag_bag.dags.items():
         globals()[dag_id] = dag

# makes dagbags globally available 
'''